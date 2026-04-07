import importlib
from unittest.mock import patch, MagicMock


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
        assert mock_run.call_count == 11
        called_sectors = [c.args[0][3] for c in mock_run.call_args_list]
        assert "Energy" in called_sectors
        assert "Information Technology" in called_sectors
        assert "Utilities" in called_sectors
