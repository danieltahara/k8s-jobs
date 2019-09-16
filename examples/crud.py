import logging
from pathlib import Path
import sys

from k8s_jobs.k8s.job import JobManager, JobGenerator, JobSigner, YamlFileConfigSource

from kubernetes import config

logging.root.setLevel(logging.NOTSET)


def create(name, signer):
    source = YamlFileConfigSource(f"{Path(__file__).parents[0]}/k8s/{name}.yaml")
    generator = JobGenerator(source)

    manager = JobManager("default", signer, {name: generator})

    job_name = manager.create_job(name)
    logging.info(f"Created job {job_name}")


def delete(signer, timeout):
    manager = JobManager("default", signer, {})

    manager.delete_old_jobs(timeout)


if __name__ == "__main__":
    config.load_kube_config()

    action = sys.argv[1]

    signer = JobSigner("signer")

    manager = JobManager("default", signer, {})
    jobs = list(manager.fetch_jobs())
    logging.info(f"Jobs: {[job.metadata.name for job in jobs]}")

    if action == "create":
        name = sys.argv[2]
        create(name, signer)
    elif action == "delete":
        timeout = int(sys.argv[2])
        delete(signer, timeout)

    jobs = list(manager.fetch_jobs())
    logging.info(f"Jobs: {[job.metadata.name for job in jobs]}")
