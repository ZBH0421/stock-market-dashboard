import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pytest
from unittest.mock import patch
from signal_checker import compute_signal


def _snap(sectors):
    """Build a minimal snapshot dict."""
    return {"date": "2026-04-07", "week_index": 0, "sectors": sectors}


def _sector(name, quadrant, rs_momentum=105.0):
    return {"sector": name, "rs_ratio": 100.0, "rs_momentum": rs_momentum,
            "quadrant": quadrant, "return_13w": 0.0, "return_4w": 0.0}


ALL_11 = [
    _sector("Communication Services", "Leading"),
    _sector("Consumer Discretionary", "Leading"),
    _sector("Consumer Staples", "Leading"),
    _sector("Energy", "Leading"),
    _sector("Financials", "Improving"),
    _sector("Health Care", "Leading"),
    _sector("Industrials", "Improving"),
    _sector("Information Technology", "Leading"),
    _sector("Materials", "Improving"),
    _sector("Real Estate", "Lagging", rs_momentum=98.0),
    _sector("Utilities", "Lagging"),
]


def test_strong_signal_when_vix_high_and_lagging_low():
    # Lagging=2 (Real Estate + Utilities), Improving=3 (Fin/Ind/Mat), VIX=25
    snapshot = _snap(ALL_11)
    with patch("signal_checker._fetch_vix", return_value=25.0):
        result = compute_signal([snapshot])
    assert result["level"] == "strong"
    assert result["lagging"] == 2
    assert result["improving"] == 3
    assert result["vix"] == 25.0
    assert result["key_momentum_signals"] == 2  # Industrials + Materials (Real Estate mom<100)
    assert "updated_at" in result
    assert "message" in result


def test_general_signal_when_vix_low():
    # Same sectors but VIX=15 → can't be strong, check general
    snapshot = _snap(ALL_11)
    with patch("signal_checker._fetch_vix", return_value=15.0):
        result = compute_signal([snapshot])
    assert result["level"] == "general"


def test_no_signal_when_lagging_high():
    sectors = [
        _sector("Communication Services", "Lagging"),
        _sector("Consumer Discretionary", "Lagging"),
        _sector("Consumer Staples", "Lagging"),
        _sector("Energy", "Lagging"),
        _sector("Financials", "Improving"),
        _sector("Health Care", "Leading"),
        _sector("Industrials", "Improving"),
        _sector("Information Technology", "Leading"),
        _sector("Materials", "Improving"),
        _sector("Real Estate", "Lagging"),
        _sector("Utilities", "Lagging"),
    ]
    with patch("signal_checker._fetch_vix", return_value=30.0):
        result = compute_signal([_snap(sectors)])
    assert result["level"] == "none"


def test_strong_requires_vix_above_20():
    # Lagging=2 but VIX=19.9 → not strong
    snapshot = _snap(ALL_11)
    with patch("signal_checker._fetch_vix", return_value=19.9):
        result = compute_signal([snapshot])
    assert result["level"] != "strong"


def test_vix_fetch_failure_falls_back_to_general_or_none():
    # VIX fetch throws → vix=None, never strong
    # ALL_11 has Lagging=2, Improving=3, key_mom=2 so general conditions ARE met
    snapshot = _snap(ALL_11)
    with patch("signal_checker._fetch_vix", side_effect=Exception("network error")):
        result = compute_signal([snapshot])
    assert result["level"] == "general"
    assert result["vix"] is None


def test_empty_snapshots_returns_none():
    result = compute_signal([])
    assert result["level"] == "none"


def test_general_signal_fires_at_lagging_3():
    # lagging=3 is still within the general threshold (<=3)
    sectors = [
        _sector("Communication Services", "Lagging"),
        _sector("Consumer Discretionary", "Leading"),
        _sector("Consumer Staples", "Leading"),
        _sector("Energy", "Leading"),
        _sector("Financials", "Improving"),
        _sector("Health Care", "Leading"),
        _sector("Industrials", "Improving"),
        _sector("Information Technology", "Leading"),
        _sector("Materials", "Improving"),
        _sector("Real Estate", "Leading"),
        _sector("Utilities", "Leading"),
    ]
    with patch("signal_checker._fetch_vix", return_value=15.0):
        result = compute_signal([_snap(sectors)])
    assert result["level"] == "general"
    assert result["lagging"] == 1


def test_no_signal_when_lagging_4():
    sectors = [
        _sector("Communication Services", "Lagging"),
        _sector("Consumer Discretionary", "Lagging"),
        _sector("Consumer Staples", "Lagging"),
        _sector("Energy", "Lagging"),
        _sector("Financials", "Improving"),
        _sector("Health Care", "Leading"),
        _sector("Industrials", "Improving"),
        _sector("Information Technology", "Leading"),
        _sector("Materials", "Improving"),
        _sector("Real Estate", "Leading"),
        _sector("Utilities", "Leading"),
    ]
    with patch("signal_checker._fetch_vix", return_value=15.0):
        result = compute_signal([_snap(sectors)])
    assert result["level"] == "none"
    assert result["lagging"] == 4


from fastapi.testclient import TestClient
from api import app

api_client = TestClient(app)


def test_api_signal_status_returns_200():
    with patch("signal_checker._fetch_vix", return_value=15.0):
        res = api_client.get("/api/signal-status")
    assert res.status_code == 200
    data = res.json()
    assert "level" in data
    assert data["level"] in ("strong", "general", "none")
    assert "lagging" in data
    assert "improving" in data
    assert "vix" in data
    assert "message" in data
    assert "updated_at" in data


def test_api_signal_status_level_is_valid_string():
    with patch("signal_checker._fetch_vix", return_value=22.0):
        res = api_client.get("/api/signal-status")
    assert res.status_code == 200
    assert res.json()["level"] in ("strong", "general", "none")
