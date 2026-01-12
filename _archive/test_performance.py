import time
from api import get_industry_data

def test_timed(name):
    print(f"Testing {name}...")
    start = time.time()
    try:
        data = get_industry_data(name)
        end = time.time()
        print(f"SUCCESS: {len(data['stocks'])} stocks found in {end-start:.2f} seconds.")
    except Exception as e:
        print(f"FAILED: {e}")

if __name__ == "__main__":
    test_timed("Airlines")
    test_industry_list = ["Biotehnology", "Bnks - Regionl", "Semiondutors"]
    for ind in test_industry_list:
        test_timed(ind)
