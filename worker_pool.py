"""Process-local worker selection without health polling or extra network calls."""
import threading
from contextlib import contextmanager


class WorkerPool:
    def __init__(self, urls):
        self.urls = tuple(dict.fromkeys(url.rstrip('/') for url in urls if url))
        self._active = dict.fromkeys(self.urls, 0)
        self._cursor = 0
        self._lock = threading.Lock()

    @contextmanager
    def reserve(self, count=1):
        if count < 1 or count > len(self.urls):
            raise RuntimeError('Not enough distinct worker URLs are configured')
        with self._lock:
            # Rotating ties avoids always sending sequential jobs to worker 1.
            rotated = self.urls[self._cursor:] + self.urls[:self._cursor]
            selected = sorted(rotated, key=self._active.__getitem__)[:count]
            self._cursor = (self.urls.index(selected[-1]) + 1) % len(self.urls)
            for url in selected:
                self._active[url] += 1
        try:
            yield selected
        finally:
            with self._lock:
                for url in selected:
                    self._active[url] -= 1
