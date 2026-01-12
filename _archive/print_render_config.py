from dotenv import load_dotenv
import os

# Load the local .env file
load_dotenv()

print("\n" + "="*50)
print("   RENDER CONFIGURATION HELPER")
print("="*50)
print("Please copy these exact values to Render Environment Variables:\n")

keys = ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME', 'DB_PORT']

for key in keys:
    val = os.getenv(key)
    if val:
        print(f"{key}: {val}")
    else:
        print(f"{key}: [MISSING IN .ENV] - Please check your .env file!")

print("\n" + "="*50 + "\n")
