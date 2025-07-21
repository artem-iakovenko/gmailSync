import subprocess
import time

while True:
    subprocess.run(["python", "main.py"])
    print("\nCompleted! 12h min pause")
    time.sleep(12 * 60)
