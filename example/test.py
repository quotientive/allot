import sys
import time

N = 120
def print_progress(i: int):
    print(f"\x02{i}/{N}\x03", flush=True)

print(f"arguments: {sys.argv}")
print_progress(0)

for i in range(N):
    print(f"this is iteration {i}")
    print_progress(i+1)
    time.sleep(1)

print_progress(N)
print("finished")
