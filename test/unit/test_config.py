from k8s_jobs.config import JobManagerFactory, ReloadingJobDefinitionsRegister


class TestEnvJobManagerFactory:
    def test_job_definition_from_env(self):
        f = JobManagerFactory.from_env()
        assert f is None


class TestReloadingJobDefinitionsRegister:
    def test_reloads_job_definitions(self):
        assert False

    def test_static_spec(self):
        assert False

    def test_file_spec(self):
        assert False
