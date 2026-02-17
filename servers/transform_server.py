import json
from datetime import datetime
from mcp.server.fastmcp import FastMCP

MCP = FastMCP("transform-server")

DATE_FIELDS = ["offense_date", "report_date"]

def _parse_date(s: str) -> str | None:
    if not s or not isinstance(s, str):
        return None
    # Try common Socrata formats
    fmts = [
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(s.replace("Z", ""), fmt)
            return dt.isoformat()
        except ValueError:
            continue
    return None

@MCP.tool()
def clean_dates(data: str) -> str:
    """
    Parse and standardize date fields to ISO format when possible.
    Adds *_parsed fields and leaves originals intact.
    """
    try:
        rows = json.loads(data)
        if not isinstance(rows, list):
            raise ValueError("Input JSON must be a list")
    except Exception as e:
        raise ValueError(f"Invalid JSON input to clean_dates: {e}")

    for r in rows:
        if not isinstance(r, dict):
            continue
        for f in DATE_FIELDS:
            parsed = _parse_date(r.get(f))
            r[f"{f}_parsed"] = parsed
    return json.dumps(rows)

@MCP.tool()
def categorize_incidents(data: str, categories: list[str]) -> str:
    """
    Group incidents into broader categories using simple keyword matching.
    Adds field: category
    """
    if not categories or not isinstance(categories, list):
        raise ValueError("categories must be a non-empty list of strings")

    try:
        rows = json.loads(data)
        if not isinstance(rows, list):
            raise ValueError("Input JSON must be a list")
    except Exception as e:
        raise ValueError(f"Invalid JSON input to categorize_incidents: {e}")

    # very simple mapping by keywords:
    # category string itself is used as a keyword (case-insensitive)
    lowered = [c.lower() for c in categories]

    for r in rows:
        if not isinstance(r, dict):
            continue
        text = " ".join([
            str(r.get("incident_type") or ""),
            str(r.get("incident_description") or ""),
        ]).lower()

        chosen = "other"
        for c in lowered:
            if c in text:
                chosen = c
                break
        r["category"] = chosen

    return json.dumps(rows)

@MCP.tool()
def detect_anomalies(data: str) -> str:
    """
    Identify potential data quality issues.
    Returns JSON with counts + sample problem rows.
    """
    try:
        rows = json.loads(data)
        if not isinstance(rows, list):
            raise ValueError("Input JSON must be a list")
    except Exception as e:
        raise ValueError(f"Invalid JSON input to detect_anomalies: {e}")

    missing_type = []
    missing_dates = []
    bad_coords = []

    for r in rows:
        if not isinstance(r, dict):
            continue

        if not (r.get("incident_type") or "").strip():
            missing_type.append(r)

        if not (r.get("offense_date") or r.get("report_date")):
            missing_dates.append(r)

        lat = r.get("latitude")
        lon = r.get("longitude")
        try:
            if lat is not None and lon is not None:
                latf = float(lat)
                lonf = float(lon)
                if not (-90 <= latf <= 90 and -180 <= lonf <= 180):
                    bad_coords.append(r)
        except Exception:
            bad_coords.append(r)

    report = {
        "total_rows": len(rows),
        "missing_incident_type": {"count": len(missing_type), "sample": missing_type[:3]},
        "missing_dates": {"count": len(missing_dates), "sample": missing_dates[:3]},
        "bad_coordinates": {"count": len(bad_coords), "sample": bad_coords[:3]},
    }
    return json.dumps(report, indent=2)

if __name__ == "__main__":
    MCP.run()
