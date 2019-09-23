import io
import logging
from pathlib import Path
import threading
from typing import Callable, Generator, Optional

logger = logging.getLogger(__name__)


class FileReloader:
    """
    Class that reloads file content if there has been a change
    """

    def __init__(self, path: str):
        self._lock = threading.Lock()
        self._path = path
        self._last_modified: float = 0

    def maybe_update(
        self
    ) -> Generator[io.TextIOBase, Optional[Callable[[], None]], None]:
        """
        Checks for an update to the file, and if it detects one, yields a text stream
        reader. Expects a callable to be sent for invocation under a lock, if there was
        no conflicting update. The callable should contain the callers update logic if
        they wish to retain the underlying thread-safety of this object.
        """
        try:
            statbuf = Path(self._path).stat()
        except FileNotFoundError:
            logger.warning(f"Could not find job_definitions_file {self._path}")
            return

        with self._lock:
            last_modified = self._last_modified
            if statbuf.mtime <= last_modified:
                return

        # Note that this read is not atomic with the statbuf check, since we don't want
        # to do IO under a lock, hence the CAS below.
        with open(self._file, "r") as f:
            update_callback = yield f

        with self._lock:
            # CAS
            if last_modified == self._last_modified:
                self._last_modified = statbuf.mtime
                if update_callback:
                    update_callback()

        # To satisfy the send()
        yield None
