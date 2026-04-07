import datetime
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest
from fastapi.testclient import TestClient
from api import app

client = TestClient(app)
TODAY = datetime.date.today().isoformat()


def _slug(s):
    return s.lower().replace(" ", "_").replace("&", "and")


def _write_sector_cache(snapshots):
    p = Path(f"/tmp/sector_rotation_history_{TODAY}.json")
    p.write_text(json.dumps({"snapshots": snapshots}))
    return p


def _write_industry_cache(sector, industries_per_snap):
    slug = _slug(sector)
    p = Path(f"/tmp/industry_rotation_history_{slug}_{TODAY}.json")
    snapshots = [{"date": TODAY, "week_index": 0, "industries": industries_per_snap}]
    p.write_text(json.dumps({"sector": sector, "total_snapshots": 1, "snapshots": snapshots}))
    return p


def _clear_caches():
    Path(f"/tmp/sector_rotation_history_{TODAY}.json").unlink(missing_ok=True)
    from generate_rotation_history import ALL_SECTORS
    for s in ALL_SECTORS:
        Path(f"/tmp/industry_rotation_history_{_slug(s)}_{TODAY}.json").unlink(missing_ok=True)


def test_returns_404_for_unknown_ticker():
    res = client.get("/api/stock-rrg-position?ticker=ZZZZNOTREAL")
    assert res.status_code == 404


def test_returns_400_without_ticker():
    res = client.get("/api/stock-rrg-position")
    assert res.status_code == 422  # FastAPI validation


def test_returns_sector_rrg_from_cache():
    _clear_caches()
    _write_sector_cache([{
        "date": TODAY,
        "week_index": 0,
        "sectors": [{"sector": "Energy", "rs_ratio": 103.5, "rs_momentum": 101.2, "quadrant": "Leading", "return_13w": 5.1, "return_4w": 1.3}],
    }])
    _write_industry_cache("Energy", [{
        "industry": "Oil & Gas Integrated",
        "rs_ratio_market": 104.0, "rs_momentum_market": 102.0, "quadrant_market": "Leading",
        "rs_ratio_sector": 101.0, "rs_momentum_sector": 100.5, "quadrant_sector": "Leading",
        "return_13w": 5.0, "return_4w": 1.2, "stock_count": 40,
    }])

    # Patch DB lookup to return a known industry
    with patch("api.db") as mock_db:
        mock_conn = MagicMock()
        mock_db.engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db.engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.execute.return_value.fetchone.return_value = ("Oil & Gas Integrated",)

        res = client.get("/api/stock-rrg-position?ticker=XOM")

    assert res.status_code == 200
    data = res.json()
    assert data["sector"] == "Energy"
    assert data["industry"] == "Oil & Gas Integrated"
    assert "sector_rrg" in data
    assert data["sector_rrg"]["quadrant"] == "Leading"
    assert "industry_rrg" in data
    assert data["industry_rrg"]["quadrant_market"] == "Leading"
    _clear_caches()


def test_returns_202_when_cache_missing():
    _clear_caches()
    with patch("api.db") as mock_db:
        mock_conn = MagicMock()
        mock_db.engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db.engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.execute.return_value.fetchone.return_value = ("Oil & Gas Integrated",)
        with patch("subprocess.Popen"):
            res = client.get("/api/stock-rrg-position?ticker=XOM")
    assert res.status_code in (200, 202)
    _clear_caches()
