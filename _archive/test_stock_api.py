
import requests
import json

BASE_URL = "http://127.0.0.1:8000"

def test_stock_api():
    ticker = "AAPL"
    url = f"{BASE_URL}/api/stock/{ticker}"
    
    print(f"Testing API: {url}")
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            print("\n[SUCCESS] API Response Received")
            print(f"Items: {list(data.keys())}")
            
            info = data.get('info', {})
            print(f"\nInfo for {info.get('symbol')}:")
            print(f"  Price: {info.get('price')}")
            print(f"  Change: {info.get('change_percent')}%")
            print(f"  News Count: {len(data.get('news', []))}")
            print(f"  History Points: {len(data.get('history', []))}")
        else:
            print(f"\n[FAIL] Status Code: {response.status_code}")
            print(response.text)
    except Exception as e:
        print(f"\n[ERROR] Request failed: {e}")
        print("Make sure the API server is running!")

if __name__ == "__main__":
    test_stock_api()
