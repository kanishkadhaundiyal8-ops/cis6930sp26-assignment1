import json
import os
from servers.load_server import save_to_sqlite, query_database, generate_summary, DB_PATH

def test_save_and_query(tmp_path, monkeypatch):
    # redirect DB to temp
    monkeypatch.setattr("servers.load_server.DB_PATH", tmp_path / "incidents.db")

    data = json.dumps([{"incident_type": "X", "category": "x"}])
    resp = json.loads(save_to_sqlite(data, "incidents"))
    assert resp["rows_saved"] == 1

    rows = json.loads(query_database("SELECT COUNT(*) as n FROM incidents"))
    assert rows[0]["n"] == 1

    summ = json.loads(generate_summary("incidents"))
    assert summ["total_rows"] == 1
