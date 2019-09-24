import secrets
import time

import git
import pytest

from k8s_jobs.exceptions import NotFoundException
from k8s_jobs.manager import JobManager, JobSigner, StaticJobDefinitionsRegister
from k8s_jobs.spec import JobGenerator, YamlFileSpecSource

REPO = git.Repo(".", search_parent_directories=True)
EXAMPLES_ROOT = REPO.working_tree_dir + "/examples/k8s"
ALL_JOB_DEFINITION_NAMES = ["job-helloworld", "job-fail", "job-timeout", "job-template"]


pytestmark = [
    pytest.mark.k8s_itest,
    pytest.mark.usefixtures("k8s_fixture"),
    pytest.mark.timeout(30),
]


@pytest.fixture(scope="module")
def register():
    def make_generator(name):
        return JobGenerator(YamlFileSpecSource(f"{EXAMPLES_ROOT}/{name}.yaml"))

    register = StaticJobDefinitionsRegister(
        {name: make_generator(name) for name in ALL_JOB_DEFINITION_NAMES}
    )
    yield register


@pytest.fixture()
def manager(request, register):
    # Add randomness here to make it so we can re-run locally without cleaning up
    signer = JobSigner(secrets.token_hex(16))
    manager = JobManager("default", signer, register=register)
    try:
        yield manager
    finally:
        # But try to clean them up anyway
        for job in manager.fetch_jobs():
            manager.delete_job(job)


def wait_for_completion(manager: JobManager, job_name: str):
    while not manager.job_is_complete(job_name):
        time.sleep(1)


def wait_for_deletion(manager: JobManager):
    while len(manager.list_jobs()) > 0:
        time.sleep(1)


class TestManager:
    def test_crud(self, manager):
        all_job_names = []

        for job_definition_name in ALL_JOB_DEFINITION_NAMES:
            # Create
            job_name = manager.create_job(
                job_definition_name, template_args={"templatevar": "xyz"}
            )
            all_job_names.append(job_name)

            # Read
            _ = manager.read_job(job_name)
            _ = manager.job_logs(job_name)
            jobs = manager.list_jobs(job_definition_name=job_definition_name)
            assert len(jobs) == 1, "Should only have one job for the job_definition"
            assert jobs[0].metadata.name == job_name, "Should return the one we created"
            wait_for_completion(manager, job_name)
            _ = manager.read_job(job_name)
            _ = manager.job_logs(job_name)

        # Delete
        manager.delete_old_jobs(retention_period_sec=0)
        wait_for_deletion(manager)

    def test_delete_old_jobs(self, manager):
        NUM_JOBS = 3

        job_names = [manager.create_job("job-helloworld") for i in range(NUM_JOBS)]

        assert len(manager.list_jobs()) == NUM_JOBS

        for job_name in job_names:
            wait_for_completion(manager, job_name)

        manager.delete_old_jobs(retention_period_sec=3600)
        assert len(manager.list_jobs()) == NUM_JOBS

        manager.delete_old_jobs(retention_period_sec=0)
        wait_for_deletion(manager)

    def test_not_found(self, manager):
        with pytest.raises(NotFoundException):
            manager.read_job("foobar")
