import os
import sys
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "serving"))
from api import app

client = TestClient(app)

def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

@patch("api.get_conn")
def test_live_utilization(mock_get_conn):
    # Setup mock
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    mock_cursor.fetchall.return_value = [
        {"zone": "Downtown", "active_vehicles": 10, "idle_ratio": 0.2, "total_fare": 150.0}
    ]

    response = client.get("/live/utilization")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["zone"] == "Downtown"
    assert data[0]["active_vehicles"] == 10

@patch("api.get_conn")
def test_profitability_report_not_found(mock_get_conn):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    mock_cursor.fetchall.return_value = [] # No data

    response = client.get("/reports/profitability/999")
    assert response.status_code == 404
    assert "No profitability report" in response.json()["detail"]
