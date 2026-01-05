import os
import subprocess
import time

def force_kill_processes():
    print("--- STARTING CLEANUP PROTOCOL ---")
    
    # Commands to kill
    targets = [
        "backfill_market_cap.py",
        "uvicorn"
    ]
    
    for target in targets:
        print(f"Hunting for processes containing: '{target}'...")
        # WMIC is powerful on Windows for this. 
        # format: wmic process where "name='python.exe' and commandline like '%TARGET%'" call terminate
        
        cmd = f'wmic process where "name=\'python.exe\' and commandline like \'%{target}%\'" call terminate'
        
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if "ReturnValue = 0;" in result.stdout:
                print(f" -> SUCCESS: Killed {target} instances.")
            elif "No Instance(s) Available" in result.stdout:
                print(f" -> Clean: No running instances of {target} found.")
            else:
                print(f" -> RESULT: {result.stdout.strip()}")
                
        except Exception as e:
            print(f" -> ERROR executing kill command: {e}")

    print("\n--- CLEANUP COMPLETE ---")
    print("Please verify the terminal processes are actually gone.")

if __name__ == "__main__":
    force_kill_processes()
