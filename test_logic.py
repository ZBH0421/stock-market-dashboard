from api import get_industry_data
import json

def test_industry(name):
    print(f"\n--- Testing Industry: {name} ---")
    try:
        data = get_industry_data(name)
        print(f"Name: {data['industry']}")
        print(f"Ticker Count: {data['ticker_count']}")
        print(f"First Stock: {data['stocks'][0]['symbol'] if data['stocks'] else 'None'}")
    except Exception as e:
        print(f"FAILED: {e}")

if __name__ == "__main__":
    # Test a few from the DB list
    test_industry("Airlines")
    test_industry("Biotehnology")
    test_industry("Bnks - Regionl")
    test_industry("Software") # Should fail
