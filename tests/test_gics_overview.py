import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi.testclient import TestClient
from api import app

client = TestClient(app)


def test_gics_overview_default_period():
    response = client.get("/api/gics-overview")
    assert response.status_code == 200
    data = response.json()
    assert "items" in data
    assert isinstance(data["items"], list)


def test_gics_overview_returns_industry_fields():
    response = client.get("/api/gics-overview?period=1W")
    assert response.status_code == 200
    items = response.json()["items"]
    if items:
        item = items[0]
        assert "industry" in item
        assert "pct_change" in item
        assert "avg_pe" in item
        assert "stock_count" in item


def test_gics_overview_invalid_period_returns_400():
    response = client.get("/api/gics-overview?period=INVALID")
    assert response.status_code == 400


def test_gics_overview_all_valid_periods():
    for period in ["1D", "1W", "1M", "3M", "6M", "1Y", "5Y"]:
        response = client.get(f"/api/gics-overview?period={period}")
        assert response.status_code == 200, f"Failed for period={period}"
        assert "items" in response.json()
