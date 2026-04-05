"""
generate_brief.py — Daily Intelligence Brief generator.

Reads:
  - ~/projects/browser_research/daily_log.json  (today's tagged news)
  - ~/projects/stockwatchlist/portfolio.json    (holdings)

Produces:
  - /tmp/brief_{date}.json                      (cached brief for API)

Usage:
  venv/bin/python generate_brief.py             # generate today's brief
  venv/bin/python generate_brief.py --force     # force regenerate
"""

import json
import sys
import argparse
import datetime
import os
import re
import time
from pathlib import Path

# ── paths ──────────────────────────────────────────────────────────────────
BASE = Path(__file__).parent
DAILY_LOG  = Path.home() / "projects/browser_research/daily_log.json"
PORTFOLIO  = Path.home() / "projects/stockwatchlist/portfolio.json"
ENV_FILE   = Path.home() / "projects/browser_research/.env"
CACHE_DIR  = Path("/tmp")

# ── env ────────────────────────────────────────────────────────────────────
def _load_env():
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

_load_env()
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
GROQ_MODEL   = os.environ.get("GROQ_MODEL", "qwen/qwen3-32b")


# ── LLM ────────────────────────────────────────────────────────────────────
def call_groq(prompt: str, max_tokens: int = 4000) -> str:
    from groq import Groq
    # Try groq-manager proxy first, fall back to direct
    for base_url in ["http://localhost:8090", None]:
        try:
            kwargs = {"api_key": GROQ_API_KEY}
            if base_url:
                kwargs["base_url"] = base_url
            client = Groq(**kwargs)
            resp = client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=max_tokens,
            )
            text = resp.choices[0].message.content.strip()
            # strip <think> blocks
            text = re.sub(r"<think>.*?</think>\s*", "", text, flags=re.DOTALL)
            text = re.sub(r"<think>.*$", "", text, flags=re.DOTALL)
            return text.strip()
        except Exception as e:
            if base_url is None:
                raise
            print(f"  [!] groq-manager unreachable ({e}), trying direct...")
    return ""


# ── data loaders ───────────────────────────────────────────────────────────
def load_today_news() -> tuple[str, list[dict], str]:
    """
    Returns (calendar_date, entries_list, data_label).

    Weekend logic:
      - Mon–Fri: use today's news only (fall back to latest if not yet available)
      - Sat: merge Fri + Sat
      - Sun: merge Fri + Sat + Sun
    data_label describes the actual date range shown (for display).
    """
    calendar_today = datetime.date.today()
    calendar_str   = calendar_today.isoformat()
    weekday = calendar_today.weekday()  # 0=Mon, 5=Sat, 6=Sun

    if not DAILY_LOG.exists():
        return calendar_str, [], calendar_str

    log = json.loads(DAILY_LOG.read_text())

    # Weekend: collect since last Friday
    if weekday in (5, 6):  # Sat or Sun
        # Find last Friday
        days_since_fri = weekday - 4  # Sat→1, Sun→2
        last_friday = calendar_today - datetime.timedelta(days=days_since_fri)
        collect_dates = []
        d = last_friday
        while d <= calendar_today:
            collect_dates.append(d.isoformat())
            d += datetime.timedelta(days=1)

        entries = []
        actual_dates = []
        for ds in collect_dates:
            day_entries = log.get(ds, [])
            if day_entries:
                entries.extend(day_entries)
                actual_dates.append(ds)

        if not entries and log:
            # No weekend data at all — use latest available
            latest = sorted(log.keys())[-1]
            entries = log[latest]
            actual_dates = [latest]

        # Deduplicate by title
        seen = set()
        deduped = []
        for e in entries:
            key = e.get("title", "")
            if key not in seen:
                seen.add(key)
                deduped.append(e)

        if len(actual_dates) > 1:
            label = f"週末彙整（{actual_dates[0]} 至 {actual_dates[-1]}）"
        elif actual_dates:
            label = actual_dates[0]
        else:
            label = calendar_str

        return calendar_str, deduped, label

    # Weekday: today only, fall back to latest if today not yet populated
    entries = log.get(calendar_str, [])
    if not entries and log:
        latest = sorted(log.keys())[-1]
        entries = log[latest]
        return calendar_str, entries, f"{latest}（最新）"

    return calendar_str, entries, calendar_str


def load_portfolio() -> list[dict]:
    if not PORTFOLIO.exists():
        return []
    return json.loads(PORTFOLIO.read_text())


def fetch_price_changes(tickers: list[str]) -> dict[str, float]:
    """Returns {ticker: pct_change_today} using yfinance."""
    if not tickers:
        return {}
    try:
        import yfinance as yf
        data = yf.download(tickers, period="2d", auto_adjust=True, progress=False)["Close"]
        pct = data.pct_change().iloc[-1] * 100
        return {str(t): round(float(v), 2) for t, v in pct.items() if str(v) != "nan"}
    except Exception as e:
        print(f"  [!] yfinance error: {e}")
        return {}


# ── brief builder ──────────────────────────────────────────────────────────
def build_ticker_stats(entries: list[dict]) -> dict[str, dict]:
    """Aggregate news entries by ticker."""
    stats = {}
    for e in entries:
        t = e["ticker"]
        if t not in stats:
            stats[t] = {"bullish": 0, "bearish": 0, "neutral": 0,
                        "articles": [], "type": e.get("type", "news")}
        sent = e.get("sentiment", "neutral")
        stats[t][sent] = stats[t].get(sent, 0) + 1
        stats[t]["articles"].append({
            "title":     e.get("title", ""),
            "sentiment": sent,
            "summary":   e.get("summary", ""),
            "url":       e.get("url", ""),
            "date":      e.get("date", ""),
        })
    return stats


def score_urgency(ticker: str, stat: dict, price_chg: float | None,
                  portfolio_map: dict) -> tuple[str, str, str]:
    """Returns (urgency_level, urgency_label, reason)."""
    total = stat["bullish"] + stat["bearish"] + stat["neutral"]
    bear_ratio = stat["bearish"] / total if total else 0
    bull_ratio = stat["bullish"] / total if total else 0
    holding = portfolio_map.get(ticker, {})
    cost    = holding.get("cost_price", 0)
    shares  = holding.get("shares", 0)
    exposure = cost * shares if cost and shares else 0

    reasons = []
    score = 0

    if price_chg is not None:
        if price_chg <= -5:
            reasons.append(f"今日大跌 {price_chg:+.1f}%")
            score += 3
        elif price_chg <= -2:
            reasons.append(f"今日下跌 {price_chg:+.1f}%")
            score += 1
        elif price_chg >= 5:
            reasons.append(f"今日大漲 {price_chg:+.1f}%")
            score += 1

    if bear_ratio >= 0.6 and total >= 3:
        reasons.append(f"{stat['bearish']}/{total} 篇看空")
        score += 2
    elif bear_ratio >= 0.4 and total >= 2:
        reasons.append(f"情緒偏空 ({stat['bearish']} 看空)")
        score += 1

    if bull_ratio >= 0.7 and total >= 3:
        reasons.append(f"{stat['bullish']}/{total} 篇看多")
        score -= 1

    if total >= 6:
        reasons.append(f"共 {total} 篇報導")
        score += 1

    if exposure >= 5000:
        score += 1

    if score >= 4:
        return "high", "🚨 立即看", "；".join(reasons) or f"{total} 篇新聞，高度關注"
    elif score >= 2:
        return "medium", "⚠️ 今天看", "；".join(reasons) or f"{total} 篇新聞"
    else:
        return "low", "✅ 無異常", "；".join(reasons) or f"{total} 篇，無重大訊號"


def build_news_digest_for_llm(ticker_stats: dict, price_changes: dict,
                               portfolio_map: dict) -> str:
    """Build a compact text digest to feed into LLM."""
    lines = []
    for ticker, stat in ticker_stats.items():
        pc = price_changes.get(ticker)
        pc_str = f"{pc:+.1f}%" if pc is not None else "N/A"
        cost = portfolio_map.get(ticker, {}).get("cost_price")
        cost_str = f"成本${cost:.2f}" if cost else ""
        lines.append(
            f"\n【{ticker}】今日{pc_str} {cost_str} "
            f"| 看多{stat['bullish']} 看空{stat['bearish']} 中性{stat['neutral']}"
        )
        for a in stat["articles"][:4]:
            sent_tag = {"bullish": "🟢", "bearish": "🔴", "neutral": "⚪"}.get(a["sentiment"], "⚪")
            lines.append(f"  {sent_tag} {a['title'][:80]}")
            if a.get("summary"):
                lines.append(f"     摘要：{a['summary'][:120]}")
    return "\n".join(lines)


def generate_llm_brief(digest: str, date_str: str) -> dict:
    """Single LLM call → returns structured dict."""
    prompt = f"""你是一位專業的股市分析師助手。以下是 {date_str} 的個股新聞彙整：

{digest}

請根據以上資料，用繁體中文產出以下 JSON 格式的市場簡報（只輸出 JSON，不要其他文字）：

{{
  "market_summary": "2-3段話的整體市場環境分析，涵蓋：今日市場主旋律、宏觀背景、投資人需注意的方向",
  "market_sentiment": "bearish 或 neutral 或 bullish",
  "themes": [
    {{
      "title": "主題名稱（簡短）",
      "sentiment": "bearish 或 neutral 或 bullish",
      "tickers": ["相關股票列表"],
      "description": "這個主題的說明（1-2句）"
    }}
  ],
  "ai_second_opinion": "以投資顧問的角度，針對今日數據給出一個最重要的行動建議或風險提醒（2-3句）"
}}

themes 請提取 2-4 個跨股票的主題。ai_second_opinion 要直接、具體、有用。
只輸出純 JSON，不要 markdown code block。"""

    raw = call_groq(prompt, max_tokens=2000)

    # Extract JSON if wrapped
    match = re.search(r'\{[\s\S]*\}', raw)
    if match:
        raw = match.group(0)

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        print(f"  [!] JSON parse failed, raw:\n{raw[:300]}")
        return {
            "market_summary": raw[:500] if raw else "LLM 分析暫時無法取得",
            "market_sentiment": "neutral",
            "themes": [],
            "ai_second_opinion": "",
        }


# ── main ───────────────────────────────────────────────────────────────────
def generate(force: bool = False) -> Path:
    today, entries, data_label = load_today_news()
    # Cache always uses calendar date so API can find it reliably
    cache_path = CACHE_DIR / f"brief_{today}.json"

    if not force and cache_path.exists():
        age = time.time() - cache_path.stat().st_mtime
        if age < 7200:  # 2 hours
            print(f"[brief] Using cached brief ({age/60:.0f} min old): {cache_path}")
            return cache_path

    print(f"[brief] Generating brief for {today} / data: {data_label} ({len(entries)} news entries)...")

    portfolio = load_portfolio()
    portfolio_map = {p["ticker"]: p for p in portfolio}

    if not entries:
        brief = {
            "date": today,
            "data_label": data_label,
            "generated_at": datetime.datetime.now().isoformat(),
            "market_summary": "今日尚無新聞資料。",
            "market_sentiment": "neutral",
            "themes": [],
            "ai_second_opinion": "",
            "priorities": [],
            "ticker_details": {},
            "price_changes": {},
        }
        cache_path.write_text(json.dumps(brief, ensure_ascii=False, indent=2))
        return cache_path

    ticker_stats = build_ticker_stats(entries)
    news_tickers = list(ticker_stats.keys())

    # All portfolio tickers for price fetch
    all_tickers = list(set(news_tickers + [p["ticker"] for p in portfolio]))
    print(f"[brief] Fetching prices for {len(all_tickers)} tickers...")
    price_changes = fetch_price_changes(all_tickers)

    # LLM brief
    print(f"[brief] Calling LLM ({GROQ_MODEL})...")
    digest = build_news_digest_for_llm(ticker_stats, price_changes, portfolio_map)
    llm_data = generate_llm_brief(digest, data_label)

    # Build priority list
    priorities = []
    for ticker, stat in ticker_stats.items():
        pc = price_changes.get(ticker)
        urgency, label, reason = score_urgency(ticker, stat, pc, portfolio_map)
        total = stat["bullish"] + stat["bearish"] + stat["neutral"]
        priorities.append({
            "ticker":       ticker,
            "urgency":      urgency,
            "urgency_label": label,
            "reason":       reason,
            "news_count":   total,
            "bullish":      stat["bullish"],
            "bearish":      stat["bearish"],
            "neutral":      stat["neutral"],
            "price_change": pc,
        })
    # Sort: high → medium → low, then by bearish count desc
    order = {"high": 0, "medium": 1, "low": 2}
    priorities.sort(key=lambda x: (order[x["urgency"]], -x["bearish"]))

    # Build ticker details (for expandable section)
    ticker_details = {}
    for ticker, stat in ticker_stats.items():
        ticker_details[ticker] = {
            "bullish":      stat["bullish"],
            "bearish":      stat["bearish"],
            "neutral":      stat["neutral"],
            "price_change": price_changes.get(ticker),
            "cost_price":   portfolio_map.get(ticker, {}).get("cost_price"),
            "shares":       portfolio_map.get(ticker, {}).get("shares"),
            "articles":     stat["articles"],
        }

    # Portfolio positions NOT in today's news (for price-only view)
    portfolio_prices = {}
    for p in portfolio:
        t = p["ticker"]
        if t not in ticker_details and t in price_changes:
            portfolio_prices[t] = {
                "price_change": price_changes[t],
                "cost_price":   p.get("cost_price"),
                "shares":       p.get("shares"),
                "name":         p.get("name", t),
            }

    brief = {
        "date":            today,
        "data_label":      data_label,
        "generated_at":    datetime.datetime.now().isoformat(),
        "news_count":      len(entries),
        "tickers_covered": news_tickers,
        "market_summary":  llm_data.get("market_summary", ""),
        "market_sentiment": llm_data.get("market_sentiment", "neutral"),
        "themes":          llm_data.get("themes", []),
        "ai_second_opinion": llm_data.get("ai_second_opinion", ""),
        "priorities":      priorities,
        "ticker_details":  ticker_details,
        "portfolio_prices": portfolio_prices,
        "price_changes":   price_changes,
    }

    cache_path.write_text(json.dumps(brief, ensure_ascii=False, indent=2))
    print(f"[brief] Saved to {cache_path}")
    return cache_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="Force regenerate")
    args = parser.parse_args()
    path = generate(force=args.force)
    data = json.loads(path.read_text())
    print(f"\n✅ Brief ready: {data['date']}")
    print(f"   Market: {data['market_sentiment']}")
    print(f"   Themes: {len(data['themes'])}")
    print(f"   Priorities: {len(data['priorities'])} tickers")
    print(f"   Summary: {data['market_summary'][:120]}...")
