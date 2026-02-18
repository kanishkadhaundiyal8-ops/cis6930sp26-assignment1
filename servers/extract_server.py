from __future__ import annotations

import json
from typing import Any, Dict, List

import requests
from mcp.server.fastmcp import FastMCP

MCP = FastMCP("extract-server")

SODA_URL = "https://data.cityofgainesville.org/resource/gvua-xt9q.json"


def _http_get(params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Always use requests params to avoid broken URLs.
    Use raise_for_status() so tests can mock it cleanly.
    """
    try:
        r = requests.get(SODA_URL, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list):
            raise ValueError("API did not return a list")
        return data
    except requests.RequestException as e:
        raise RuntimeError(f"HTTP error from Gainesville API: {e}") from e


@MCP.tool()
def fetch_incidents(limit: int = 100, offset: int = 0) -> str:
    """Fetch incident data from the Gainesville Crime Responses dataset."""
    if limit < 1 or limit > 2000:
        return json.dumps({"ok": False, "error": "limit must be between 1 and 2000"}, indent=2)
    if offset < 0:
        return json.dumps({"ok": False, "error": "offset must be >= 0"}, indent=2)

    params = {"$limit": limit, "$offset": offset}
    try:
        data = _http_get(params)
        return json.dumps(data, indent=2)
    except Exception as e:
        return json.dumps({"ok": False, "error": str(e)}, indent=2)


@MCP.tool()
def get_incident_types(limit: int = 500, offset: int = 0) -> str:
    """Return a unique list of incident/narrative types seen in a sample window."""
    params = {"$limit": limit, "$offset": offset}
    try:
        rows = _http_get(params)
        vals = set()
        for r in rows:
            # dataset often uses "narrative" rather than "incident_type"
            v = r.get("incident_type") or r.get("narrative")
            if v:
                vals.add(str(v))
        return json.dumps(sorted(vals), indent=2)
    except Exception as e:
        return json.dumps({"ok": False, "error": str(e)}, indent=2)


@MCP.tool()
def fetch_by_date_range(start_iso: str, end_iso: str, limit: int = 100, offset: int = 0) -> str:
    """
    Fetch incidents between a report_date window.
    ISO examples: 2026-02-01T00:00:00.000
    """
    if limit < 1 or limit > 2000:
        return json.dumps({"ok": False, "error": "limit must be between 1 and 2000"}, indent=2)
    if offset < 0:
        return json.dumps({"ok": False, "error": "offset must be >= 0"}, indent=2)

    where = f"report_date between '{start_iso}' and '{end_iso}'"
    params = {"$limit": limit, "$offset": offset, "$where": where}

    try:
        rows = _http_get(params)
        return json.dumps(rows, indent=2)
    except Exception as e:
        return json.dumps({"ok": False, "error": str(e)}, indent=2)


@MCP.resource("schema://incidents")
def get_schema() -> str:
    """Return schema + API notes for the dataset."""
    schema = {
        "source": SODA_URL,
        "notes": "Field names are defined by the Socrata dataset and may evolve. Use fetch_incidents() to inspect live keys.",
        "expected_common_fields": [
            "incident_type",
            "narrative",
            "report_date",
            "offense_date",
            "case_number",
            "address",
            "latitude",
            "longitude",
            "location",
            "status",
        ],
        "pagination": {"limit_param": "$limit", "offset_param": "$offset", "max_limit": 2000},
    }
    return json.dumps(schema, indent=2)


if __name__ == "__main__":
    MCP.run()

