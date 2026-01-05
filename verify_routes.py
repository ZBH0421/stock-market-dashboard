from api import app

print("--- Registered Routes ---")
for route in app.routes:
    print(f"{route.methods} {route.path}")
    
print("\n--- Checking Galaxy ---")
has_galaxy = any(r.path == "/api/galaxy" for r in app.routes)
if has_galaxy:
    print("SUCCESS: /api/galaxy is registered.")
else:
    print("FAILURE: /api/galaxy is MISSING.")
