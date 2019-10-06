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
        self.path = path
        self._lock = threading.Lock()
        self._last_modified: float = 0

    # NOTE: ctime/mtime-based change detection is slightly racy depending on the
    # filesystem precision.
    def maybe_reload(
        self
    ) -> Generator[io.TextIOBase, Optional[Callable[[], None]], None]:
        """
        Checks for an update to the file, and if it detects one, yields a text stream
        reader. Expects a callable to be sent for invocation under a lock, if there was
        no conflicting update. The callable should contain the callers update logic if
        they wish to retain the underlying thread-safety of this object.
        """
        try:
            statbuf = Path(self.path).stat()
        except FileNotFoundError:
            logger.warning(f"Could not find job_definitions_file {self.path}")
            return

        with self._lock:
            last_modified = self._last_modified
            if statbuf.st_mtime <= last_modified:
                return

        logger.info(f"Processing update to {self.path}")

        # Note that this read is not atomic with the statbuf check, since we don't want
        # to do IO under a lock, hence the CAS below.
        with open(self.path, "r") as f:
            update_callback = yield f

        with self._lock:
            # CAS
            if last_modified == self._last_modified:
                self._last_modified = statbuf.st_mtime
                if update_callback:
                    update_callback()

        # To satisfy the send()
        yield None
