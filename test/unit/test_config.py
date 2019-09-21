import os
from unittest.mock import Mock

from k8s_jobs.config import EnvJobManagerFactory


class TestEnvJobManagerFactory:
    def test_job_definition_config_path(self):
        config_root = "/foo/bar"
        job_definition_name = "jd1"
        f = EnvJobManagerFactory(
            "namespace", "signature", config_root, {job_definition_name: Mock()}
        )

        assert (
            f.job_definition_config_path(job_definition_name)
            == config_root + "/" + job_definition_name
        )

    def test_job_definition_config_path_override(self):
        job_definition_name = "jd1"
        f = EnvJobManagerFactory(
            "namespace", "signature", "foo", {job_definition_name: Mock()}
        )
        custom_path = "/custom/path"

        os.environ[
            EnvJobManagerFactory.JOB_DEFINITION_PATH_ENV_PREFIX + "JD1"
        ] = custom_path
        try:
            assert f.job_definition_config_path(job_definition_name) == custom_path
        finally:
            del os.environ[EnvJobManagerFactory.JOB_DEFINITION_PATH_ENV_PREFIX + "JD1"]

        assert f.job_definition_config_path(job_definition_name) != custom_path
