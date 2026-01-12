from api import get_galaxy_data
import json

print("--- Debugging Galaxy API ---")
try:
    data = get_galaxy_data()
    print(f"Total Stars: {data.get('count')}")
    stars = data.get('stars', [])
    if stars:
        print("First Star Sample:")
        print(json.dumps(stars[0], indent=2))
        
        # Check for NaN madness which breaks JSON
        import math
        invalid = [s for s in stars if not (s['x_pe'] > -99999)] # simple check
        
    else:
        print("Stars list is empty!")

except Exception as e:
    print(f"API Failed: {e}")
    import traceback
    traceback.print_exc()
