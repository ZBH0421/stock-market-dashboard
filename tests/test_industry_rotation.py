import sys, os, json, datetime, subprocess
from pathlib import Path
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
