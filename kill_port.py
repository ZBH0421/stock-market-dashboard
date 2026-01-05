import subprocess
import re
import os

def kill_port_8000():
    print("--- OPERATION: LIBERATE PORT 8000 ---")
    
    # 1. Find PID using netstat
    # Output format: TCP    127.0.0.1:8000    0.0.0.0:0    LISTENING    1234
    cmd = "netstat -ano | findstr :8000"
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        lines = result.stdout.strip().split('\n')
        
        pids = set()
        for line in lines:
            if "LISTENING" in line:
                parts = line.split()
                pid = parts[-1]
                pids.add(pid)
        
        if not pids:
            print("Status: Port 8000 is already free. No ghosts detected.")
            return

        print(f"Detected Ghost Processes holding Port 8000: {pids}")
        
        # 2. Kill PIDs
        for pid in pids:
            print(f"Targeting PID {pid}...")
            kill_cmd = f"taskkill /F /PID {pid}"
            subprocess.run(kill_cmd, shell=True)
            print(f" -> PID {pid} Terminated.")
            
    except Exception as e:
        print(f"Error during operation: {e}")

if __name__ == "__main__":
    kill_port_8000()
