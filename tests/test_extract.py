import json
import requests
from unittest.mock import patch, Mock
from servers.extract_server import fetch_incidents

@patch("servers.extract_server.requests.get")
def test_fetch_incidents_ok(mock_get):
    m = Mock()
    m.raise_for_status.return_value = None
    m.json.return_value = [{"case_number": "123"}]
    mock_get.return_value = m

    out = fetch_incidents(limit=1, offset=0)
    rows = json.loads(out)
    assert isinstance(rows, list)
    assert rows[0]["case_number"] == "123"
