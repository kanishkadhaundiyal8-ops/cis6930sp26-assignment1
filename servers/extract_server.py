from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import requests
from loguru import logger
from mcp.server.fastmcp import FastMCP

MCP = FastMCP("extract_server")

# Gainesville Crime Responses dataset (Socrata)
SODA_URL = "https://data.cityofgainesville.org/resource/gvua-xt9q.json"


def _http_get(params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    HTTP GET helper.

    IMPORTANT (for tests):
    - In unit tests, requests.get() is mocked and may not provide a numeric status_code.
    - So we rely on raise_for_status() instead of comparing status_code >= 400.
    """
    try:
        r = requests.get(SODA_URL, params=params, timeout=30)
        # Works both in real requests and in mocked tests
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list):
            raise ValueError("Unexpected response: expected a JSON list")
        return data
    except requests.HTTPError as e:
        # Best-effort status code extraction (may be missing in mocks)
        status = getattr(getattr(e, "response", None), "status_code", None)
        msg = f"HTTP error from Gainesville API (status={status}): {e}"
        logger.error(msg)
        raise
    except Exception as e:
        logger.error(f"Unexpected error calling Socrata API: {e}")
        raise


@MCP.resource("schema://incidents")
def get_schema() -> str:
    schema = {
        "source": SODA_URL,
        "notes": "Field names are defined by the Socrata dataset and may evolve. Use fetch_incidents() to inspect live keys.",
        "expected_common_fields": [
            "incident_type",
            "report_date",
            "offense_date",
            "case_number",
            "location",
            "latitude",
            "longitude",
            "status",
        ],
        "pagination": {
            "limit_param": "$limit",
            "offset_param": "$offset",
            "max_limit": 2000,
        },
    }
    return json.dumps(schema, indent=2)


@MCP.tool()
def fetch_incidents(limit: int = 100, offset: int = 0) -> str:
    """
    Fetch incidents with Socrata pagination ($limit, $offset).
    Returns a JSON string:
      - on success: JSON list of rows
      - on error: {"ok": false, "error": "..."}
    """
    if limit <= 0:
        return json.dumps({"ok": False, "error": "limit must be > 0"})
    if limit > 2000:
        return json.dumps({"ok": False, "error": "limit too large (max 2000)"})
    if offset < 0:
        return json.dumps({"ok": False, "error": "offset must be >= 0"})

    params = {"$limit": limit, "$offset": offset}
    try:
        data = _http_get(params)
        return json.dumps(data)
    except Exception as e:
        return json.dumps({"ok": False, "error": str(e)})


@MCP.tool()
def get_incident_types(limit: int = 200, offset: int = 0) -> str:
    """
    Return a deduplicated list of incident/narrative types.
    """
    if limit <= 0:
        return json.dumps({"ok": False, "error": "limit must be > 0"})
    if limit > 2000:
        return json.dumps({"ok": False, "error": "limit too large (max 2000)"})
    if offset < 0:
        return json.dumps({"ok": False, "error": "offset must be >= 0"})

    params = {"$limit": limit, "$offset": offset}
    try:
        rows = _http_get(params)
        types = set()
        for r in rows:
            # Dataset varies; tests use "narrative"
            val = r.get("incident_type") or r.get("narrative")
            if isinstance(val, str) and val.strip():
                types.add(val.strip())
        return json.dumps(sorted(types))
    except Exception as e:
        return json.dumps({"ok": False, "error": str(e)})


@MCP.tool()
def fetch_by_date_range(
    start_iso: str,
    end_iso: str,
    limit: int = 100,
    offset: int = 0,
) -> str:
    """
    Fetch incidents within a date range using Socrata $where.
    """
    if limit <= 0:
        return json.dumps({"ok": False, "error": "limit must be > 0"})
    if limit > 2000:
        return json.dumps({"ok": False, "error": "limit too large (max 2000)"})
    if offset < 0:
        return json.dumps({"ok": False, "error": "offset must be >= 0"})

    where = f"report_date between '{start_iso}' and '{end_iso}'"
    params = {"$where": where, "$limit": limit, "$offset": offset}

    try:
        rows = _http_get(params)
        return json.dumps(rows)
    except Exception as e:
        return json.dumps({"ok": False, "error": str(e)})


if __name__ == "__main__":
    MCP.run()

