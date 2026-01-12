from api import get_market_flow
import pandas as pd
import json

def test_flow_logic():
    print("Testing get_market_flow(days=5)...")
    try:
        # Call the function directly (bypass HTTP)
        result = get_market_flow(days=5)
        
        print(f"End Date: {result.get('end_date')}")
        items = result.get('items', [])
        print(f"Items Returned: {len(items)}")
        
        if items:
            df = pd.DataFrame(items)
            print("\nTop 5 Inflow:")
            print(df.sort_values('net_flow', ascending=False).head(5)[['industry', 'net_flow', 'total_volume']])
            
            print("\nTop 5 Outflow:")
            print(df.sort_values('net_flow', ascending=True).head(5)[['industry', 'net_flow', 'total_volume']])
            
            # Check for NaN or weird values
            if df['net_flow'].isnull().any():
                print("WARNING: Found NULL net_flow values!")
            
            print("\n[SUCCESS] Data structure is valid.")
        else:
            print("[WARNING] No items returned. Check if database has prices for the last 5 days.")

    except Exception as e:
        print(f"[ERROR] Logic failed: {e}")

if __name__ == "__main__":
    test_flow_logic()
