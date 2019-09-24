import copy
from io import StringIO
import os
from typing import Dict, List
import unittest.mock as mock
import yaml

import pytest

from k8s_jobs.config import JobManagerFactory, ReloadingJobDefinitionsRegister
from k8s_jobs.exceptions import NotFoundException


@pytest.fixture
def MockReloader():
    def make_mock_because_pytest_disallows_class_fixtures(
        return_values: List[List[Dict]]
    ):
        return_values = copy.deepcopy(return_values)

        def maybe_reload():
            if len(return_values) == 0:
                return

            r = return_values.pop()

            cb = yield StringIO(yaml.dump(r))

            cb()

            yield None

        m = mock.Mock(maybe_reload=mock.Mock(wraps=maybe_reload))
        return m

    yield make_mock_because_pytest_disallows_class_fixtures


class TestJobManagerFactory:
    @mock.patch.dict(
        os.environ,
        {
            JobManagerFactory.JOB_SIGNATURE_ENV_VAR: "xyz",
            JobManagerFactory.JOB_NAMESPACE_ENV_VAR: "notdefault",
            JobManagerFactory.JOB_DEFINITIONS_CONFIG_PATH_ENV_VAR: "/etc/foo/bar",
        },
    )
    def test_job_definition_from_env(self):
        f = JobManagerFactory.from_env()

        assert f.namespace == "notdefault"
        assert f.signature == "xyz"
        assert isinstance(f.register, ReloadingJobDefinitionsRegister)

    def test_manager(self):
        f = JobManagerFactory("ns1", "sig", mock.Mock())

        # Should not raise
        _ = f.manager()


class TestReloadingJobDefinitionsRegister:
    def test_static_spec(self, MockReloader):
        spec = {"this": "is", "a": "spec"}
        mock_reloader = MockReloader([[{"name": "foo", "spec": spec}]])

        with mock.patch(
            "k8s_jobs.config.StaticJobSpecSource"
        ) as MockStaticJobSpecSource:
            _ = ReloadingJobDefinitionsRegister(mock_reloader)

            MockStaticJobSpecSource.assert_called_once_with(spec)

    def test_file_spec(self, MockReloader):
        spec_path = "/foo/bar"
        mock_reloader = MockReloader([[{"name": "foo", "spec_path": spec_path}]])

        with mock.patch("k8s_jobs.config.YamlFileSpecSource") as MockYamlFileSpecSource:
            _ = ReloadingJobDefinitionsRegister(mock_reloader)

            MockYamlFileSpecSource.assert_called_once_with(spec_path)

    def test_reloads(self, MockReloader):
        mock_reloader = MockReloader([[]])
        register = ReloadingJobDefinitionsRegister(mock_reloader)

        mock_reloader.maybe_reload.assert_called_once()
        mock_reloader.maybe_reload.reset_mock()

        # Handles the NOP, so no exception
        _ = register.generators

        mock_reloader.maybe_reload.assert_called_once()
        mock_reloader.maybe_reload.reset_mock()

    def test_not_found(self, MockReloader):
        mock_reloader = MockReloader([[]])
        register = ReloadingJobDefinitionsRegister(mock_reloader)

        with pytest.raises(NotFoundException):
            register.get_generator("unknown")
