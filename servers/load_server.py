import json
import os
import sqlite3
from pathlib import Path
import pandas as pd
from mcp.server.fastmcp import FastMCP

MCP = FastMCP("load-server")

DB_PATH = Path("data/incidents.db")

def _ensure_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

@MCP.tool()
def save_to_sqlite(data: str, table_name: str = "incidents") -> str:
    """
    Save processed data to SQLite database.
    Returns a message with row count.
    """
    if not table_name.replace("_", "").isalnum():
        raise ValueError("table_name must be alphanumeric/underscore only")

    try:
        rows = json.loads(data)
        if not isinstance(rows, list):
            raise ValueError("Input JSON must be a list")
    except Exception as e:
        raise ValueError(f"Invalid JSON input to save_to_sqlite: {e}")

    df = pd.DataFrame(rows)
    _ensure_db()

    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)

    return json.dumps({"status": "ok", "table": table_name, "rows_saved": len(df)})

@MCP.tool()
def query_database(sql: str) -> str:
    """
    Execute a SQL query on the processed data and return results as JSON list.
    """
    if not sql or not isinstance(sql, str):
        raise ValueError("sql must be a non-empty string")

    if not DB_PATH.exists():
        raise RuntimeError("Database not found. Run save_to_sqlite first.")

    with sqlite3.connect(DB_PATH) as conn:
        try:
            cur = conn.execute(sql)
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall()
        except sqlite3.Error as e:
            raise RuntimeError(f"SQLite error: {e}")

    result = [dict(zip(cols, r)) for r in rows] if cols else []
    return json.dumps(result)

@MCP.tool()
def generate_summary(table_name: str = "incidents") -> str:
    """
    Generate summary statistics for a table.
    """
    if not DB_PATH.exists():
        raise RuntimeError("Database not found. Run save_to_sqlite first.")

    with sqlite3.connect(DB_PATH) as conn:
        try:
            total = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        except sqlite3.Error as e:
            raise RuntimeError(f"SQLite error: {e}")

        def safe_query(q):
            try:
                return conn.execute(q).fetchall()
            except sqlite3.Error:
                return []

        top_types = safe_query(
            f"""
            SELECT incident_type, COUNT(*) as n
            FROM {table_name}
            WHERE incident_type IS NOT NULL AND TRIM(incident_type) != ''
            GROUP BY incident_type
            ORDER BY n DESC
            LIMIT 10
            """
        )

        top_categories = safe_query(
            f"""
            SELECT category, COUNT(*) as n
            FROM {table_name}
            WHERE category IS NOT NULL AND TRIM(category) != ''
            GROUP BY category
            ORDER BY n DESC
            LIMIT 10
            """
        )

    summary = {
        "table": table_name,
        "total_rows": total,
        "top_incident_types": [{"incident_type": t, "count": n} for (t, n) in top_types],
        "top_categories": [{"category": c, "count": n} for (c, n) in top_categories],
    }
    return json.dumps(summary, indent=2)

if __name__ == "__main__":
    MCP.run()
