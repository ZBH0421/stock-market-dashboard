import importlib
import datetime
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from api import app

client = TestClient(app)


def _load_updater():
    import daily_update
    importlib.reload(daily_update)
    return daily_update


def test_warmup_called_for_all_sectors():
    """After _warmup_industry_cache(), subprocess.run is called for all 11 sectors."""
    du = _load_updater()

    updater = du.DailyUpdater.__new__(du.DailyUpdater)

    with patch("subprocess.run") as mock_run:
        updater._warmup_industry_cache()
        from generate_rotation_history import ALL_SECTORS
        assert mock_run.call_count == len(ALL_SECTORS)
        called_sectors = [c.args[0][3] for c in mock_run.call_args_list]
        assert "Energy" in called_sectors
        assert "Information Technology" in called_sectors
        assert "Utilities" in called_sectors


def _clear_etf_cache():
    today = datetime.date.today().isoformat()
    for period in ["3M", "6M", "1Y", "3Y", "ALL"]:
        Path(f"/tmp/sector_etf_prices_{period}_{today}.json").unlink(missing_ok=True)


def _write_fake_etf_cache(period="1Y"):
    today = datetime.date.today().isoformat()
    data = {
        "period": period,
        "sectors": {
            "Energy": {"etf": "XLE", "dates": ["2024-01-02"], "values": [0.0]},
        },
        "benchmarks": {
            "SPY": {"dates": ["2024-01-02"], "values": [0.0]},
            "QQQ": {"dates": ["2024-01-02"], "values": [0.0]},
        },
    }
    Path(f"/tmp/sector_etf_prices_{period}_{today}.json").write_text(json.dumps(data))


def test_etf_endpoint_has_benchmarks_key():
    _clear_etf_cache()
    _write_fake_etf_cache("1Y")
    res = client.get("/api/sector-etf-prices?period=1Y")
    assert res.status_code == 200
    data = res.json()
    assert "benchmarks" in data
    assert "SPY" in data["benchmarks"]
    assert "QQQ" in data["benchmarks"]
    _clear_etf_cache()


def test_etf_benchmark_has_dates_and_values():
    _clear_etf_cache()
    _write_fake_etf_cache("1Y")
    res = client.get("/api/sector-etf-prices?period=1Y")
    data = res.json()
    spy = data["benchmarks"]["SPY"]
    assert "dates" in spy
    assert "values" in spy
    assert isinstance(spy["dates"], list)
    assert isinstance(spy["values"], list)
    _clear_etf_cache()
