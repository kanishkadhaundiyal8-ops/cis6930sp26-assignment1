import json
import requests
from requests.exceptions import Timeout, ConnectionError, HTTPError, RequestException
from mcp.server.fastmcp import FastMCP

MCP = FastMCP("extract-server")

BASE_URL = "https://data.cityofgainesville.org/resource/gvua-xt9q.json"

DEFAULT_FIELDS = [
    "case_number",
    "offense_date",
    "report_date",
    "incident_type",
    "incident_description",
    "case_status",
    "address",
    "latitude",
    "longitude",
]

def _safe_get(url: str, params: dict) -> list[dict]:
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        return r.json()
    except Timeout:
        raise RuntimeError("Timeout while calling Gainesville API")
    except ConnectionError:
        raise RuntimeError("Connection error while calling Gainesville API")
    except HTTPError as e:
        status = getattr(r, "status_code", "unknown")
        raise RuntimeError(f"HTTP error from Gainesville API (status={status}): {e}")
    except ValueError:
        raise RuntimeError("Failed to parse JSON from Gainesville API")
    except RequestException as e:
        raise RuntimeError(f"Request failed: {e}")

@MCP.tool()
def fetch_incidents(limit: int = 100, offset: int = 0) -> str:
    """
    Fetch crime incident data from the Gainesville API.
    Returns a JSON string list of incident objects.
    """
    if limit < 1 or limit > 5000:
        raise ValueError("limit must be between 1 and 5000")
    if offset < 0:
        raise ValueError("offset must be >= 0")

    params = {
        "$limit": limit,
        "$offset": offset,
        "$select": ",".join(DEFAULT_FIELDS),
    }
    data = _safe_get(BASE_URL, params)
    return json.dumps(data)

@MCP.tool()
def fetch_recent_incidents(days: int = 7, limit: int = 200) -> str:
    """
    Fetch incidents from the last N days (best-effort).
    Uses Socrata date filtering on report_date when present.
    """
    if days < 1 or days > 365:
        raise ValueError("days must be between 1 and 365")
    if limit < 1 or limit > 5000:
        raise ValueError("limit must be between 1 and 5000")

    # Socrata supports ISO-8601 comparisons if report_date is a datetime string.
    # We'll let the API handle it; if field missing in some rows, those are simply excluded.
    where = f"report_date > now() - interval '{days}' day"

    params = {
        "$limit": limit,
        "$select": ",".join(DEFAULT_FIELDS),
        "$where": where,
        "$order": "report_date DESC",
    }
    data = _safe_get(BASE_URL, params)
    return json.dumps(data)

@MCP.tool()
def get_incident_types(sample_limit: int = 2000) -> str:
    """
    Get unique incident types (best-effort) by sampling.
    Returns JSON list of strings.
    """
    if sample_limit < 1 or sample_limit > 5000:
        raise ValueError("sample_limit must be between 1 and 5000")

    params = {
        "$limit": sample_limit,
        "$select": "incident_type",
    }
    rows = _safe_get(BASE_URL, params)
    types = sorted({(r.get("incident_type") or "").strip() for r in rows if r.get("incident_type")})
    return json.dumps(types)

@MCP.resource("schema://incidents")
def get_schema() -> str:
    """
    Return a simple schema description for the incident records we use.
    """
    schema = {
        "source": BASE_URL,
        "fields": DEFAULT_FIELDS,
        "notes": [
            "Dates are provided as strings by the API (may be missing).",
            "Latitude/longitude may be rounded/blurred (Marsy's Law).",
            "Some fields can be null/empty depending on incident."
        ],
    }
    return json.dumps(schema, indent=2)

if __name__ == "__main__":
    MCP.run()
