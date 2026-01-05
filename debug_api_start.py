import sys
import time

print("--- Debugging API Startup ---")
try:
    print("1. Importing api module...")
    import api
    print("   Success: api module imported.")
    
    print("2. Checking app instance...")
    if hasattr(api, 'app'):
        print("   Success: app object found.")
    else:
        print("   ERROR: app object NOT found in api.")
        
    print("3. Testing DB Connection from api.db...")
    with api.db.engine.connect() as conn:
        print("   Success: Connected to DB.")
        
    print("\n[OK] API checks out. The server should be runnable.")
    
except ImportError as e:
    print(f"\n[CRITICAL ERROR] Import failed: {e}")
except Exception as e:
    print(f"\n[CRITICAL ERROR] Startup failed: {e}")
    import traceback
    traceback.print_exc()
