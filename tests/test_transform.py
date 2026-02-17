import json
from servers.transform_server import clean_dates, categorize_incidents, detect_anomalies

def test_clean_dates_adds_parsed_fields():
    data = json.dumps([{"report_date": "2024-01-01"}])
    out = json.loads(clean_dates(data))
    assert "report_date_parsed" in out[0]

def test_categorize_incidents_adds_category():
    data = json.dumps([{"incident_description": "happy theft"}])
    out = json.loads(categorize_incidents(data, ["theft", "assault"]))
    assert "category" in out[0]

def test_detect_anomalies_returns_report():
    data = json.dumps([{"incident_type": ""}])
    report = json.loads(detect_anomalies(data))
    assert "total_rows" in report
