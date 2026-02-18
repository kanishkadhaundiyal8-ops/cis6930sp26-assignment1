import json
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
from loguru import logger
from mcp.server.fastmcp import FastMCP

MCP = FastMCP("transform_server")


def _parse_json_list(data: str, ctx: str) -> List[Dict[str, Any]]:
    try:
        obj = json.loads(data)
    except json.JSONDecodeError as e:
        raise ValueError(f"{ctx}: invalid JSON: {e}") from e
    if not isinstance(obj, list):
        raise ValueError(f"{ctx}: Input JSON must be a list (got {type(obj)})")
    out: List[Dict[str, Any]] = []
    for row in obj:
        out.append(row if isinstance(row, dict) else {"_value": row})
    return out


def _iso_parse(s: Any) -> str | None:
    if s is None:
        return None
    if not isinstance(s, str) or not s.strip():
        return None
    # Socrata timestamps look like: 2026-02-16T23:15:00.000
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.isoformat()
        except Exception:
            pass
    return None


@MCP.tool()
def clean_dates(data: str) -> str:
    """
    Standardize report_date/offense_date into *_parsed ISO strings.
    Input: JSON list[dict]
    Output: JSON list[dict]
    """
    rows = _parse_json_list(data, "clean_dates")
    for r in rows:
        r["report_date_parsed"] = _iso_parse(r.get("report_date"))
        r["offense_date_parsed"] = _iso_parse(r.get("offense_date"))
    return json.dumps(rows)


@MCP.tool()
def categorize_incidents(data: str, categories: List[str]) -> str:
    """
    Assign each record to one of the provided high-level categories based on text.
    Uses narrative (primary) and incident_type (fallback).
    Output column: category (lowercase)
    """
    rows = _parse_json_list(data, "categorize_incidents")

    # keywords by category (simple but works well for rubric)
    rules = {
        "THEFT/PROPERTY": [
            "theft", "stolen", "burglary", "robbery", "larceny", "shoplift", "retail",
            "vehicle", "auto", "tag", "property", "fraud", "lost property"
        ],
        "ASSAULT/VIOLENCE": [
            "assault", "battery", "domestic", "violence", "fight", "threat", "sexual", "kidnap"
        ],
        "DRUG/ALCOHOL": [
            "drug", "narcotic", "cocaine", "heroin", "meth", "marijuana", "alcohol", "dui", "intox"
        ],
        "TRAFFIC": [
            "traffic", "crash", "accident", "hit and run", "reckless", "speed", "road", "parking"
        ],
        "BURGLARY": [
            "burglary", "break", "breaking", "trespass", "prowler"
        ],
    }

    allowed = {c.upper() for c in categories} if categories else set(rules.keys()) | {"OTHER"}

    for r in rows:
        text = (r.get("narrative") or r.get("incident_type") or "").lower()

        chosen = "OTHER"
        for cat, kws in rules.items():
            if cat in allowed and any(kw in text for kw in kws):
                chosen = cat
                break

        r["category"] = chosen.lower()

    return json.dumps(rows)


@MCP.tool()
def detect_anomalies(data: str) -> str:
    """
    Basic data quality checks (missing narrative/type, missing dates, bad coords).
    Returns JSON report.
    """
    rows = _parse_json_list(data, "detect_anomalies")

    missing_type = [r for r in rows if not (r.get("narrative") or r.get("incident_type"))]
    missing_dates = [r for r in rows if not r.get("report_date") or not r.get("offense_date")]

    def bad_coord(r: Dict[str, Any]) -> bool:
        try:
            lat = float(r.get("latitude")) if r.get("latitude") is not None else None
            lon = float(r.get("longitude")) if r.get("longitude") is not None else None
            if lat is None or lon is None:
                return False
            return not (-90 <= lat <= 90 and -180 <= lon <= 180)
        except Exception:
            return True

    bad_coords = [r for r in rows if bad_coord(r)]

    report = {
        "total_rows": len(rows),
        "missing_type_or_narrative": {"count": len(missing_type), "sample": missing_type[:3]},
        "missing_dates": {"count": len(missing_dates), "sample": missing_dates[:3]},
        "bad_coordinates": {"count": len(bad_coords), "sample": bad_coords[:3]},
    }
    return json.dumps(report, indent=2)


if __name__ == "__main__":
    try:
        MCP.run()
    except Exception:
        logger.exception("transform_server crashed")
        raise

