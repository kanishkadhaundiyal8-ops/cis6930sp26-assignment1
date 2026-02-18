from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from loguru import logger
from mcp.server.fastmcp import FastMCP

MCP = FastMCP("load-server")

DB_PATH = Path("data/incidents.db")


def _ensure_db_dir():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def _parse_json_list(data: str, ctx: str) -> List[Dict[str, Any]]:
    try:
        obj = json.loads(data)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON input to {ctx}: {e}") from e

    if not isinstance(obj, list):
        raise ValueError("Input JSON must be a list")
    for i, row in enumerate(obj):
        if not isinstance(row, dict):
            raise ValueError(f"Row {i} is not an object")
    return obj


def _make_sqlite_safe(df: pd.DataFrame) -> pd.DataFrame:
    """
    SQLite can't store dict/list directly -> convert dict/list values to JSON strings.
    """
    for col in df.columns:
        if df[col].map(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
    return df


@MCP.tool()
def save_to_sqlite(data: str, table_name: str) -> str:
    """
    Save processed data (JSON list[object]) to SQLite.
    Returns JSON: {ok, db_path, table, rows_saved, columns} or {ok:false, error,...}
    """
    _ensure_db_dir()

    try:
        rows = _parse_json_list(data, "save_to_sqlite")
        if not table_name or not table_name.replace("_", "").isalnum():
            return json.dumps({"ok": False, "error": "Invalid table_name"})

        df = pd.DataFrame(rows)
        df = _make_sqlite_safe(df)

        with sqlite3.connect(DB_PATH) as conn:
            df.to_sql(table_name, conn, if_exists="replace", index=False)

        return json.dumps(
            {
                "ok": True,
                "db_path": str(DB_PATH.resolve()),
                "table": table_name,
                "rows_saved": int(len(df)),
                "columns": list(df.columns),
            },
            indent=2,
        )
    except Exception as e:
        logger.exception("save_to_sqlite failed")
        return json.dumps(
            {"ok": False, "error": f"{type(e).__name__}: {e}", "db_path": str(DB_PATH.resolve())},
            indent=2,
        )


@MCP.tool()
def query_database(sql: str) -> str:
    """
    Execute SQL and return results as JSON list[dict].
    (Matches tests that expect rows[0]["n"] style access.)
    """
    if not DB_PATH.exists():
        return json.dumps([{"error": "Database not found. Run save_to_sqlite first."}], indent=2)

    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.execute(sql)
            rows = cur.fetchall()
            out = [dict(r) for r in rows]
        return json.dumps(out, indent=2)
    except Exception as e:
        return json.dumps([{"error": f"{type(e).__name__}: {e}"}], indent=2)


@MCP.tool()
def generate_summary(table_name: str) -> str:
    """
    Generate summary statistics for a table.
    IMPORTANT: tests expect "total_rows" key.
    """
    if not DB_PATH.exists():
        return json.dumps({"ok": False, "error": "Database not found. Run save_to_sqlite first."}, indent=2)

    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.execute(f"SELECT COUNT(*) AS n FROM {table_name};")
            total_rows = int(cur.fetchone()[0])

            cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table_name});").fetchall()]

            # pick a reasonable column for top values
            top_col = None
            for candidate in ["incident_type", "narrative", "category"]:
                if candidate in cols:
                    top_col = candidate
                    break

            top_values = []
            if top_col:
                q = f"""
                SELECT {top_col} AS value, COUNT(*) AS c
                FROM {table_name}
                GROUP BY {top_col}
                ORDER BY c DESC
                LIMIT 10;
                """
                top_values = conn.execute(q).fetchall()
                top_values = [[r[0], r[1]] for r in top_values]

            # date ranges (best effort)
            date_ranges = {}
            for dcol in ["report_date", "offense_date", "report_date_parsed", "offense_date_parsed"]:
                if dcol in cols:
                    r = conn.execute(f"SELECT MIN({dcol}), MAX({dcol}) FROM {table_name};").fetchone()
                    date_ranges[dcol] = {"min": r[0], "max": r[1]}

        return json.dumps(
            {
                "ok": True,
                "db_path": str(DB_PATH.resolve()),
                "table": table_name,
                "total_rows": total_rows,   # <-- REQUIRED by your test
                "row_count": total_rows,    # keep this too (nice for your pipeline output)
                "top_values": top_values,
                "date_ranges": date_ranges,
            },
            indent=2,
        )
    except Exception as e:
        return json.dumps({"ok": False, "error": f"{type(e).__name__}: {e}"}, indent=2)


if __name__ == "__main__":
    MCP.run()

