import sys, os, json, datetime
from pathlib import Path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi.testclient import TestClient
from api import app

client = TestClient(app)


def _write_fake_cache(snapshots: list) -> Path:
    today = datetime.date.today().isoformat()
    cache = Path(f"/tmp/sector_rotation_history_{today}.json")
    cache.write_text(json.dumps({
        "generated_at": today,
        "total_snapshots": len(snapshots),
        "snapshots": snapshots,
    }))
    return cache


def _clear_cache():
    today = datetime.date.today().isoformat()
    Path(f"/tmp/sector_rotation_history_{today}.json").unlink(missing_ok=True)
    Path(f"/tmp/sector_rotation_history_{today}.lock").unlink(missing_ok=True)


def test_returns_200_when_cache_exists():
    fake_snapshot = {
        "date": "2025-01-01",
        "week_index": 0,
        "sectors": [{"sector": "Energy", "rs_ratio": 105.0, "rs_momentum": 102.0, "quadrant": "Leading", "return_13w": 5.0, "return_4w": 2.0}],
    }
    _write_fake_cache([fake_snapshot])
    try:
        res = client.get("/api/sector-rotation-history")
        assert res.status_code == 200
        data = res.json()
        assert "snapshots" in data
        assert data["total_snapshots"] == 1
        assert data["snapshots"][0]["date"] == "2025-01-01"
    finally:
        _clear_cache()


def test_returns_202_when_no_cache():
    _clear_cache()
    res = client.get("/api/sector-rotation-history")
    assert res.status_code == 202
    assert res.json()["status"] == "generating"


def test_snapshot_sector_fields():
    fake_snapshot = {
        "date": "2025-06-01",
        "week_index": 0,
        "sectors": [
            {"sector": "Energy", "rs_ratio": 110.5, "rs_momentum": 104.3, "quadrant": "Leading", "return_13w": 8.2, "return_4w": 3.1},
            {"sector": "Financials", "rs_ratio": 92.1, "rs_momentum": 97.0, "quadrant": "Lagging", "return_13w": -4.1, "return_4w": -1.2},
        ],
    }
    _write_fake_cache([fake_snapshot])
    try:
        res = client.get("/api/sector-rotation-history")
        assert res.status_code == 200
        s = res.json()["snapshots"][0]["sectors"][0]
        assert "sector" in s
        assert "rs_ratio" in s
        assert "rs_momentum" in s
        assert "quadrant" in s
        assert "return_13w" in s
        assert "return_4w" in s
        assert isinstance(s["rs_ratio"], float)
        assert isinstance(s["return_13w"], float)
    finally:
        _clear_cache()
