import sys, os, json, datetime, subprocess
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def _slug(sector): return sector.lower().replace(' ', '_').replace('&', 'and')

def _clear_industry_cache(sector):
    today = datetime.date.today().isoformat()
    slug = _slug(sector)
    Path(f"/tmp/industry_rotation_history_{slug}_{today}.json").unlink(missing_ok=True)
    Path(f"/tmp/industry_rotation_history_{slug}_{today}.lock").unlink(missing_ok=True)

def test_invalid_sector_exits_nonzero():
    result = subprocess.run(
        ["venv/bin/python", "generate_industry_rotation_history.py", "--sector", "FakeSector"],
        capture_output=True, text=True,
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    assert result.returncode != 0

def test_generates_cache_file():
    sector = "Energy"
    _clear_industry_cache(sector)
    try:
        result = subprocess.run(
            ["venv/bin/python", "generate_industry_rotation_history.py", "--sector", sector, "--force"],
            capture_output=True, text=True, timeout=300,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        assert result.returncode == 0, result.stderr
        today = datetime.date.today().isoformat()
        cache = Path(f"/tmp/industry_rotation_history_{_slug(sector)}_{today}.json")
        assert cache.exists()
        data = json.loads(cache.read_text())
        assert data["sector"] == sector
        assert data["total_snapshots"] > 0
        snap = data["snapshots"][0]
        assert "date" in snap and "week_index" in snap and "industries" in snap
        ind = snap["industries"][0]
        for field in ["industry", "rs_ratio_market", "rs_momentum_market", "quadrant_market",
                      "rs_ratio_sector", "rs_momentum_sector", "quadrant_sector",
                      "return_13w", "return_4w", "stock_count"]:
            assert field in ind, f"Missing field: {field}"
        assert ind["quadrant_market"] in ("Leading", "Weakening", "Lagging", "Improving")
    finally:
        _clear_industry_cache(sector)

from fastapi.testclient import TestClient
from api import app

client = TestClient(app)


def _write_industry_cache(sector, snapshots):
    today = datetime.date.today().isoformat()
    slug = _slug(sector)
    cache = Path(f"/tmp/industry_rotation_history_{slug}_{today}.json")
    cache.write_text(json.dumps({
        "generated_at": today,
        "sector": sector,
        "total_snapshots": len(snapshots),
        "snapshots": snapshots,
    }))
    return cache


def _fake_snapshot():
    return {
        "date": "2025-01-01",
        "week_index": 0,
        "industries": [{
            "industry": "Oil & Gas Exploration & Production",
            "rs_ratio_market": 105.0, "rs_momentum_market": 102.0,
            "quadrant_market": "Leading",
            "rs_ratio_sector": 103.0, "rs_momentum_sector": 101.0,
            "quadrant_sector": "Leading",
            "return_13w": 5.0, "return_4w": 2.0, "stock_count": 42,
        }],
    }


def test_api_returns_200_with_cache():
    _clear_industry_cache("Energy")
    _write_industry_cache("Energy", [_fake_snapshot()])
    try:
        res = client.get("/api/industry-rotation-history?sector=Energy")
        assert res.status_code == 200
        data = res.json()
        assert data["sector"] == "Energy"
        assert data["total_snapshots"] == 1
    finally:
        _clear_industry_cache("Energy")


def test_api_returns_202_without_cache():
    _clear_industry_cache("Energy")
    with patch("subprocess.Popen"):
        res = client.get("/api/industry-rotation-history?sector=Energy")
    assert res.status_code == 202
    assert res.json()["status"] == "generating"


def test_api_returns_400_for_invalid_sector():
    res = client.get("/api/industry-rotation-history?sector=FakeSector")
    assert res.status_code == 400


def test_api_snapshot_fields():
    _clear_industry_cache("Energy")
    _write_industry_cache("Energy", [_fake_snapshot()])
    try:
        res = client.get("/api/industry-rotation-history?sector=Energy")
        assert res.status_code == 200
        ind = res.json()["snapshots"][0]["industries"][0]
        for field in ["industry", "rs_ratio_market", "rs_momentum_market", "quadrant_market",
                      "rs_ratio_sector", "rs_momentum_sector", "quadrant_sector",
                      "return_13w", "return_4w", "stock_count"]:
            assert field in ind, f"Missing: {field}"
    finally:
        _clear_industry_cache("Energy")
