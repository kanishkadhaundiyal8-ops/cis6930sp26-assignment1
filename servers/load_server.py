import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from loguru import logger
from mcp.server.fastmcp import FastMCP

MCP = FastMCP("load_server")

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "incidents.db"


def _ensure_db_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _parse_json_list(data: str, ctx: str) -> List[Dict[str, Any]]:
    try:
        obj = json.loads(data)
    except json.JSONDecodeError as e:
        raise ValueError(f"{ctx}: invalid JSON: {e}") from e

    if not isinstance(obj, list):
        raise ValueError(f"{ctx}: Input JSON must be a list (got {type(obj)})")

    cleaned: List[Dict[str, Any]] = []
    for row in obj:
        if isinstance(row, dict):
            cleaned.append(row)
        else:
            cleaned.append({"_value": row})
    return cleaned


def _connect() -> sqlite3.Connection:
    _ensure_db_dir()
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def _stringify_complex_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert any column values that are dict/list into JSON strings so SQLite can store them.
    """
    for col in df.columns:
        # find at least one complex value in this column
        has_complex = False
        for v in df[col].head(50).tolist():
            if isinstance(v, (dict, list)):
                has_complex = True
                break

        if has_complex:
            df[col] = df[col].apply(
                lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x
            )
    return df


@MCP.tool()
def save_to_sqlite(data: str, table_name: str) -> str:
    """
    Save processed data (JSON list[dict]) to SQLite under data/incidents.db
    """
    try:
        if not table_name or not table_name.replace("_", "").isalnum():
            raise ValueError("table_name must be alphanumeric/underscore only")

        rows = _parse_json_list(data, "save_to_sqlite")
        df = pd.DataFrame(rows)

        # convert dict/list values (e.g., location) into JSON strings
        df = _stringify_complex_columns(df)

        conn = _connect()
        try:
            df.to_sql(table_name, conn, if_exists="replace", index=False)
        finally:
            conn.close()

        msg = {
            "ok": True,
            "db_path": str(DB_PATH),
            "table": table_name,
            "rows_saved": int(len(df)),
            "columns": list(df.columns),
        }
        return json.dumps(msg, indent=2)

    except Exception as e:
        logger.exception("save_to_sqlite failed")
        return json.dumps(
            {"ok": False, "error": f"{type(e).__name__}: {str(e)}", "db_path": str(DB_PATH)},
            indent=2,
        )


@MCP.tool()
def query_database(sql: str) -> str:
    """
    Run a SQL query and return rows as JSON.
    """
    try:
        if not DB_PATH.exists():
            return json.dumps({"ok": False, "error": "Database not found. Run save_to_sqlite first."}, indent=2)

        conn = _connect()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            cols = [d[0] for d in cur.description] if cur.description else []
            out_rows = cur.fetchall() if cur.description else []
        finally:
            conn.close()

        return json.dumps({"ok": True, "columns": cols, "rows": out_rows}, indent=2)

    except Exception as e:
        logger.exception("query_database failed")
        return json.dumps({"ok": False, "error": f"{type(e).__name__}: {str(e)}"}, indent=2)


@MCP.tool()
def generate_summary(table_name: str) -> str:
    """
    Simple summary stats: row count, top narratives (or incident types if present), date min/max if possible.
    """
    try:
        if not DB_PATH.exists():
            return json.dumps({"ok": False, "error": "Database not found. Run save_to_sqlite first."}, indent=2)

        conn = _connect()
        try:
            cur = conn.cursor()

            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cur.fetchone()[0]

            # If incident_type doesn't exist (your data uses narrative), fall back
            top_field = None
            for candidate in ["incident_type", "narrative", "category"]:
                try:
                    cur.execute(f"SELECT {candidate} FROM {table_name} LIMIT 1")
                    top_field = candidate
                    break
                except Exception:
                    continue

            top_vals = []
            if top_field:
                cur.execute(
                    f"""
                    SELECT {top_field}, COUNT(*) as c
                    FROM {table_name}
                    WHERE {top_field} IS NOT NULL
                    GROUP BY {top_field}
                    ORDER BY c DESC
                    LIMIT 10
                    """
                )
                top_vals = cur.fetchall()

            date_range = {}
            for col in ["report_date", "offense_date", "report_date_parsed", "offense_date_parsed"]:
                try:
                    cur.execute(f"SELECT MIN({col}), MAX({col}) FROM {table_name} WHERE {col} IS NOT NULL")
                    mn, mx = cur.fetchone()
                    if mn is not None or mx is not None:
                        date_range[col] = {"min": mn, "max": mx}
                except Exception:
                    continue

        finally:
            conn.close()

        return json.dumps(
            {
                "ok": True,
                "db_path": str(DB_PATH),
                "table": table_name,
                "row_count": count,
                "top_values": top_vals,
                "date_ranges": date_range,
            },
            indent=2,
        )

    except Exception as e:
        logger.exception("generate_summary failed")
        return json.dumps({"ok": False, "error": f"{type(e).__name__}: {str(e)}"}, indent=2)


if __name__ == "__main__":
    MCP.run()

