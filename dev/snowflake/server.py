"""Start the pinned emulator and seed it once through its public SQL API."""
import signal
import subprocess
import time
import urllib.request
from sql import seed

process = subprocess.Popen(['/app/snowflake-emulator'])
def stop(signum, _frame):
    process.send_signal(signum)
for signum in (signal.SIGTERM, signal.SIGINT):
    signal.signal(signum, stop)
try:
    deadline=time.monotonic()+30
    while True:
        if process.poll() is not None:
            raise RuntimeError('Snowflake emulator exited during startup')
        try:
            urllib.request.urlopen('http://127.0.0.1:64616/health',timeout=1).close()
            break
        except OSError:
            if time.monotonic() >= deadline:
                raise RuntimeError('Snowflake emulator did not become ready')
            time.sleep(.1)
    seed()
    raise SystemExit(process.wait())
finally:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
