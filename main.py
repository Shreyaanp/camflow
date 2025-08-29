import subprocess, sys, time, signal

SERVICES = [
    ("tof",      [sys.executable, "services/tof_service.py"]),
    ("video",    [sys.executable, "services/video_service.py"]),
    ("windower", [sys.executable, "services/windower.py"]),
    ("ml",       [sys.executable, "services/ml_consumer.py"]),
]

procs = []
try:
    for name, cmd in SERVICES:
        p = subprocess.Popen(cmd)
        procs.append((name, p))
    while True:
        dead = [n for n, p in procs if p.poll() is not None]
        if dead:
            print("Exited:", dead)
            break
        time.sleep(2)
finally:
    for name, p in procs:
        try: p.send_signal(signal.SIGINT)
        except: pass
    for _, p in procs:
        try: p.wait(timeout=5)
        except: pass
