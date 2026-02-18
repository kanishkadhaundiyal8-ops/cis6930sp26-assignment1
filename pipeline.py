import os
import json
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

import anyio
import requests
from dotenv import load_dotenv
from loguru import logger
from contextlib import AsyncExitStack

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

load_dotenv()

NAV_URL = "https://api.ai.it.ufl.edu/v1/chat/completions"
MODEL = os.getenv("NAV_MODEL", "granite-3.3-8b-instruct")

DEFAULT_TABLE = "incidents"
DEFAULT_LIMIT = 500
DEFAULT_OFFSET = 0

DEFAULT_CATEGORIES = [
    "THEFT/PROPERTY",
    "ASSAULT/VIOLENCE",
    "DRUG/ALCOHOL",
    "TRAFFIC",
    "BURGLARY",
    "OTHER",
]


# ---------------- NavigatorAI (optional) ----------------
def call_llm(messages: List[Dict[str, str]]) -> str:
    api_key = os.getenv("NAVIGATOR_API_KEY")
    if not api_key:
        raise RuntimeError("NAVIGATOR_API_KEY missing. Put it in .env (do not commit .env).")

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {"model": MODEL, "messages": messages}
    r = requests.post(NAV_URL, headers=headers, json=payload, timeout=60)
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]


# ---------------- JSON helpers ----------------
def _try_json_loads(s: str) -> Any:
    try:
        return json.loads(s)
    except Exception:
        return None


def try_parse_json(text: Any) -> Any:
    if text is None:
        return None
    if isinstance(text, (dict, list)):
        return text

    s = str(text).strip()
    if not s:
        return s

    parsed = _try_json_loads(s)
    if parsed is not None:
        return parsed

    m = re.search(r"```(?:json)?\s*(\{.*\}|\[.*\])\s*```", s, flags=re.DOTALL)
    if m:
        parsed = _try_json_loads(m.group(1))
        if parsed is not None:
            return parsed

    return s


def ensure_list(data: Any, ctx: str) -> List[Dict[str, Any]]:
    """
    Ensures the object is a list of dicts.
    Raises with a helpful error if not.
    """
    data = try_parse_json(data)
    if isinstance(data, list):
        # also allow list of primitives, but we expect dicts
        if data and not isinstance(data[0], dict):
            raise ValueError(f"{ctx}: Expected list of objects (dicts), got list of {type(data[0])}")
        return data
    raise ValueError(f"{ctx}: Expected JSON list, got {type(data)}: {str(data)[:200]}")


def pretty(obj: Any) -> str:
    try:
        return json.dumps(obj, indent=2, ensure_ascii=False)
    except Exception:
        return str(obj)


# ---------------- MCP client helpers ----------------
async def start_server_session(stack: AsyncExitStack, py_file: str) -> ClientSession:
    params = StdioServerParameters(
        command=sys.executable,
        args=[py_file],
        env=os.environ.copy(),
    )
    read, write = await stack.enter_async_context(stdio_client(params))
    session = await stack.enter_async_context(ClientSession(read, write))
    await session.initialize()
    return session


async def call_tool(session: ClientSession, tool_name: str, args: Optional[Dict[str, Any]] = None) -> Any:
    args = args or {}
    result = await session.call_tool(tool_name, args)
    try:
        text = result.content[0].text
    except Exception:
        text = getattr(result, "content", result)
    return try_parse_json(text)


async def read_schema(session: ClientSession) -> Any:
    """
    Prefer resource schema://incidents (per assignment spec).
    Fallback to tool get_schema if you implemented it.
    """
    # Try resource first
    try:
        res = await session.read_resource("schema://incidents")
        # res.contents is typically a list; each item may have .text
        try:
            txt = res.contents[0].text
            return try_parse_json(txt)
        except Exception:
            return try_parse_json(res)
    except Exception:
        pass

    # Fallback to tool name
    try:
        return await call_tool(session, "get_schema", {})
    except Exception as e:
        return f"Schema unavailable (no resource schema://incidents and no get_schema tool). Error: {e}"


# ---------------- Planning ----------------
def build_llm_plan_prompt(schema: Any, anomalies: Any) -> str:
    return f"""
You are a data engineer orchestrator.

Given the schema and anomaly report, choose SAFE fetch parameters only.

Schema:
{schema}

Anomaly report:
{anomalies}

Return ONLY JSON with keys:
- fetch_limit (int, 100..2000)
- fetch_offset (int, >=0)

Return JSON only. No extra keys.
""".strip()


def sanitize_plan(plan: Any) -> Tuple[int, int]:
    limit = DEFAULT_LIMIT
    offset = DEFAULT_OFFSET

    if isinstance(plan, dict):
        if "fetch_limit" in plan:
            try:
                limit = int(plan["fetch_limit"])
            except Exception:
                limit = DEFAULT_LIMIT
        if "fetch_offset" in plan:
            try:
                offset = int(plan["fetch_offset"])
            except Exception:
                offset = DEFAULT_OFFSET

    limit = max(100, min(limit, 2000))
    offset = max(0, offset)
    return limit, offset


def build_safe_queries(sample_rows: List[Dict[str, Any]], table: str) -> List[str]:
    """
    Build queries based on actual columns present.
    """
    cols = set(sample_rows[0].keys()) if sample_rows else set()

    queries = [f"SELECT COUNT(*) AS total_rows FROM {table};"]

    if "category" in cols:
        queries.append(
            f"SELECT category, COUNT(*) AS n FROM {table} GROUP BY category ORDER BY n DESC LIMIT 10;"
        )

    if "incident_type" in cols:
        queries.append(
            f"SELECT incident_type, COUNT(*) AS n FROM {table} GROUP BY incident_type ORDER BY n DESC LIMIT 10;"
        )

    # common date fields in Socrata datasets vary; include if present
    date_col = None
    for c in ["report_date", "offense_date", "report_datetime", "offense_datetime"]:
        if c in cols:
            date_col = c
            break
    if date_col:
        queries.append(
            f"SELECT MIN({date_col}) AS min_date, MAX({date_col}) AS max_date FROM {table};"
        )

    return queries[:6]


# ---------------- Pipeline ----------------
async def run_pipeline() -> None:
    logger.info("Starting MCP pipeline (stdio) ...")

    async with AsyncExitStack() as stack:
        extract = await start_server_session(stack, "servers/extract_server.py")
        transform = await start_server_session(stack, "servers/transform_server.py")
        load = await start_server_session(stack, "servers/load_server.py")
        logger.info("Connected to extract/transform/load servers.")

        schema = await read_schema(extract)

        # Sample always from fetch_incidents (tool must exist per spec)
        sample_raw = await call_tool(extract, "fetch_incidents", {"limit": 100, "offset": 0})
        sample = ensure_list(sample_raw, "fetch_incidents(sample)")
        logger.info("Got schema + sample.")

        # Anomalies must receive JSON list string
        anomalies_raw = await call_tool(transform, "detect_anomalies", {"data": json.dumps(sample)})
        anomalies = try_parse_json(anomalies_raw)
        logger.info("Anomaly report created.")

        # Optional: let LLM suggest ONLY limit/offset
        limit, offset = DEFAULT_LIMIT, DEFAULT_OFFSET
        if os.getenv("NAVIGATOR_API_KEY"):
            try:
                llm_out = call_llm([{"role": "user", "content": build_llm_plan_prompt(schema, anomalies)}])
                llm_plan = try_parse_json(llm_out)
                limit, offset = sanitize_plan(llm_plan)
                logger.info(f"Using LLM fetch plan: limit={limit}, offset={offset}")
            except Exception as e:
                logger.warning(f"LLM planning failed; using default limit/offset. Error: {e}")

        table_name = DEFAULT_TABLE
        categories = DEFAULT_CATEGORIES

        logger.info(f"Plan: limit={limit}, offset={offset}, table={table_name}")

        # Fetch real batch
        raw_batch = await call_tool(extract, "fetch_incidents", {"limit": limit, "offset": offset})
        incidents = ensure_list(raw_batch, "fetch_incidents(full)")

        # Transform
        cleaned_raw = await call_tool(transform, "clean_dates", {"data": json.dumps(incidents)})
        cleaned = ensure_list(cleaned_raw, "clean_dates")

        categorized_raw = await call_tool(
            transform,
            "categorize_incidents",
            {"data": json.dumps(cleaned), "categories": categories},
        )
        categorized = ensure_list(categorized_raw, "categorize_incidents")
        logger.info("Transform complete (clean_dates + categorize_incidents).")

        # Load
        save_msg = await call_tool(load, "save_to_sqlite", {"data": json.dumps(categorized), "table_name": table_name})

        # If save failed, STOP and show report (don’t pretend pipeline succeeded)
        if isinstance(save_msg, str) and save_msg.lower().startswith("error"):
            raise RuntimeError(f"save_to_sqlite failed: {save_msg}")

        summary = await call_tool(load, "generate_summary", {"table_name": table_name})

        # Build safe queries from real columns
        safe_queries = build_safe_queries(categorized[:1], table_name)
        query_results = []
        for q in safe_queries:
            out = await call_tool(load, "query_database", {"sql": q})
            query_results.append({"sql": q, "result": out})

        print("\n================ PIPELINE REPORT ================\n")
        print("PLAN:")
        print(pretty({"limit": limit, "offset": offset, "table_name": table_name, "categories": categories}))
        print("\nSCHEMA:")
        print(pretty(schema))
        print("\nANOMALIES (sample):")
        print(pretty(anomalies))
        print("\nSAVE RESULT:")
        print(pretty(save_msg))
        print("\nSUMMARY:")
        print(pretty(summary))
        print("\nQUERIES:")
        print(pretty(query_results))
        print("\n=================================================\n")

        logger.info("Pipeline complete ✅")


def main():
    anyio.run(run_pipeline)


if __name__ == "__main__":
    main()

