from pathlib import Path
from unittest.mock import Mock
import yaml

import pytest

from k8s_jobs.file_reloader import FileReloader


class TestFileReloader:
    def test_not_found(self, request, tmp_path):
        path = tmp_path / request.node.name
        reloader = FileReloader(path)

        update = reloader.maybe_reload()
        with pytest.raises(StopIteration):
            next(update)

    def test_updates(self, request, tmp_path):
        path = tmp_path / request.node.name
        data = "foo"
        with open(path, "w+") as f:
            yaml.dump(data, f)
        reloader = FileReloader(path)

        update = reloader.maybe_reload()
        loaded_data = yaml.safe_load(next(update))

        assert loaded_data == data

    def test_no_update(self, request, tmp_path):
        path = tmp_path / request.node.name
        Path(path).touch()
        reloader = FileReloader(path)

        update = reloader.maybe_reload()
        _ = next(update)
        update.send(None)

        # Subsequent call should NOP
        with pytest.raises(StopIteration):
            update = reloader.maybe_reload()
            next(update)

    def test_callback(self, request, tmp_path):
        path = tmp_path / request.node.name
        Path(path).touch()
        reloader = FileReloader(path)
        cb = Mock()

        update = reloader.maybe_reload()
        _ = next(update)
        update.send(cb)

        cb.assert_called_once()
