"""Process-scoped macOS sleep assertions; never changes global power settings."""
import os
import subprocess
import sys
from contextlib import contextmanager


class SleepGuard:
    def __init__(self, process=None):
        self.process = process

    def check(self):
        if self.process is not None and self.process.poll() is not None:
            raise RuntimeError('SLEEP_GUARD_EXITED')

    def close(self):
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=3)


@contextmanager
def sleep_guard(enabled=True):
    guard = SleepGuard()
    if enabled and sys.platform == 'darwin':
        try:
            # -w releases assertions even if the runner crashes; display may sleep.
            guard.process = subprocess.Popen(
                ['/usr/bin/caffeinate', '-i', '-s', '-w', str(os.getpid())],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL, close_fds=True,
            )
        except OSError:
            raise RuntimeError('SLEEP_GUARD_UNAVAILABLE') from None
    try:
        guard.check()
        yield guard
    finally:
        guard.close()
