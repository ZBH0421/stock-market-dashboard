import yfinance as yf
import json

def check_news():
    bot = yf.Ticker("AAPL")
    news = bot.news
    
    print(f"--- Fetching News for AAPL ---")
    print(f"Count: {len(news)}")
    
    if news:
        print("\n--- First Article Structure ---")
        # Print keys to see what's available
        first_article = news[0]
        print(json.dumps(first_article, indent=2))
        
        has_content = 'content' in first_article or 'body' in first_article or 'text' in first_article
        print(f"\n[Analysis] Contains full content/body? {'YES' if has_content else 'NO'}")
    else:
        print("No news found.")

if __name__ == "__main__":
    check_news()
