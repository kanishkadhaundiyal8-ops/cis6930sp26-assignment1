from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from loguru import logger
from mcp.server.fastmcp import FastMCP

MCP = FastMCP("load_server")

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "incidents.db"


_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe_table_name(name: str) -> bool:
    return bool(_TABLE_RE.match(name or ""))


def _ensure_list_json(data: str) -> List[Dict[str, Any]]:
    try:
        obj = json.loads(data)
    except Exception as e:
        raise ValueError(f"Invalid JSON: {e}")
    if not isinstance(obj, list):
        raise ValueError("Input JSON must be a list")
    # Ensure rows are dict-like
    out: List[Dict[str, Any]] = []
    for i, row in enumerate(obj):
        if not isinstance(row, dict):
            raise ValueError(f"Row {i} is not an object/dict")
        out.append(row)
    return out


def _jsonify_nested(df: pd.DataFrame) -> pd.DataFrame:
    """
    SQLite can't store dict/list objects directly. Convert any dict/list cell to a JSON string.
    """
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
        )
    return df


@MCP.tool()
def save_to_sqlite(data: str, table_name: str = "incidents") -> str:
    """
    Save a JSON list of objects into SQLite (replace table each run).
    Returns a JSON object with {ok, db_path, table, rows_saved, columns} or {ok:false,error,...}
    """
    if not _safe_table_name(table_name):
        return json.dumps({"ok": False, "error": "Invalid table name", "db_path": str(DB_PATH)})

    try:
        rows = _ensure_list_json(data)
        df = pd.DataFrame(rows)
        df = _jsonify_nested(df)

        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(DB_PATH) as conn:
            df.to_sql(table_name, conn, if_exists="replace", index=False)

        return json.dumps({
            "ok": True,
            "db_path": str(DB_PATH),
            "table": table_name,
            "rows_saved": int(len(df)),
            "columns": list(df.columns),
        })
    except Exception as e:
        logger.exception("save_to_sqlite failed")
        return json.dumps({"ok": False, "error": f"{type(e).__name__}: {e}", "db_path": str(DB_PATH)})


@MCP.tool()
def query_database(sql: str) -> str:
    """
    Execute SQL and return results as JSON list[dict].
    On error, return: [{"error": "..."}]
    """
    if not DB_PATH.exists():
        return json.dumps([{"error": "Database not found. Run save_to_sqlite first."}])

    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.execute(sql)
            rows = cur.fetchall()
            out = [dict(r) for r in rows]
            return json.dumps(out)
    except Exception as e:
        return json.dumps([{"error": f"{type(e).__name__}: {e}"}])


@MCP.tool()
def generate_summary(table_name: str = "incidents") -> str:
    """
    Generate a small summary:
      - total_rows
      - top_values of incident_type/narrative
      - date_ranges for known date columns (if present)
    """
    if not _safe_table_name(table_name):
        return json.dumps({"ok": False, "error": "Invalid table name", "db_path": str(DB_PATH), "table": table_name})

    if not DB_PATH.exists():
        return json.dumps({"ok": False, "error": "Database not found. Run save_to_sqlite first.", "db_path": str(DB_PATH), "table": table_name})

    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row

            total_rows = conn.execute(f"SELECT COUNT(*) AS n FROM {table_name};").fetchone()["n"]

            # Pick the best "type" column available
            cols = [r["name"] for r in conn.execute(f"PRAGMA table_info({table_name});").fetchall()]
            type_col = "incident_type" if "incident_type" in cols else ("narrative" if "narrative" in cols else None)

            top_values: List[List[Any]] = []
            if type_col:
                q = f"""
                SELECT {type_col} AS v, COUNT(*) AS n
                FROM {table_name}
                WHERE {type_col} IS NOT NULL AND TRIM({type_col}) != ''
                GROUP BY {type_col}
                ORDER BY n DESC
                LIMIT 10;
                """
                top = conn.execute(q).fetchall()
                top_values = [[r["v"], r["n"]] for r in top]

            # Date ranges for common columns (only if present)
            date_ranges: Dict[str, Dict[str, Any]] = {}
            for dcol in ["report_date", "offense_date", "report_date_parsed", "offense_date_parsed"]:
                if dcol in cols:
                    r = conn.execute(
                        f"SELECT MIN({dcol}) AS min_date, MAX({dcol}) AS max_date FROM {table_name};"
                    ).fetchone()
                    date_ranges[dcol] = {"min": r["min_date"], "max": r["max_date"]}

            return json.dumps({
                "ok": True,
                "db_path": str(DB_PATH),
                "table": table_name,
                "total_rows": int(total_rows),   # <-- matches test expectation
                "top_values": top_values,
                "date_ranges": date_ranges,
            })
    except Exception as e:
        return json.dumps({"ok": False, "error": f"{type(e).__name__}: {e}", "db_path": str(DB_PATH), "table": table_name})


if __name__ == "__main__":
    MCP.run()

