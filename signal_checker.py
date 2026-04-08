"""
Computes RRG-based market bottom signal level from the latest weekly snapshot + VIX.

Levels:
  "strong"  — Lagging<=2, Improving>=2, VIX>20       (historical 7/7 success)
  "general" — Lagging<=3, Improving>=2, key_mom>=2   (historical ~85% success)
  "none"    — all other states
"""
import yfinance as yf

_KEY_SECTORS = {"Real Estate", "Materials", "Industrials"}


def _fetch_vix() -> float:
    """Fetch latest VIX close. Raises on failure."""
    data = yf.download("^VIX", period="5d", progress=False)
    close = data["Close"]
    if hasattr(close, "columns"):
        close = close.iloc[:, 0]
    return float(close.iloc[-1])


def compute_signal(snapshots: list) -> dict:
    """
    Given a list of snapshot dicts (from compute_snapshots()), return a signal dict.

    Args:
        snapshots: list of dicts with keys 'date', 'week_index', 'sectors'.
                   Each sector dict has 'sector', 'quadrant', 'rs_momentum'.

    Returns:
        {
          "level": "strong" | "general" | "none",
          "lagging": int,
          "improving": int,
          "vix": float | None,
          "key_momentum_signals": int,
          "message": str,
          "updated_at": str,
        }
    """
    if not snapshots:
        return _result("none", 0, 0, None, 0, "目前無底部信號", "N/A")

    latest = snapshots[-1]
    sectors = latest.get("sectors", [])
    updated_at = latest.get("date", "N/A")

    lagging = sum(1 for s in sectors if s["quadrant"] == "Lagging")
    improving = sum(1 for s in sectors if s["quadrant"] == "Improving")
    key_mom = sum(
        1 for s in sectors
        if s["sector"] in _KEY_SECTORS and s["rs_momentum"] > 100
    )

    vix = None
    try:
        vix = _fetch_vix()
    except Exception as e:
        print(f"[signal_checker] WARNING: VIX fetch failed: {e}")

    is_strong = (
        lagging <= 2
        and improving >= 2
        and vix is not None
        and vix > 20
    )
    is_general = (
        not is_strong
        and lagging <= 3
        and improving >= 2
        and key_mom >= 2
    )

    if is_strong:
        vix_str = f"{vix:.1f}"
        msg = f"底部強信號：Lagging={lagging}｜VIX={vix_str}｜歷史勝率 100%（7/7）"
        level = "strong"
    elif is_general:
        vix_str = f"{vix:.1f}" if vix is not None else "N/A"
        msg = f"底部信號：Lagging={lagging}｜VIX={vix_str}｜歷史勝率 85%"
        level = "general"
    else:
        msg = "目前無底部信號"
        level = "none"

    return _result(level, lagging, improving, vix, key_mom, msg, updated_at)


def _result(level, lagging, improving, vix, key_mom, message, updated_at):
    return {
        "level": level,
        "lagging": lagging,
        "improving": improving,
        "vix": round(vix, 2) if vix is not None else None,
        "key_momentum_signals": key_mom,
        "message": message,
        "updated_at": updated_at,
    }
