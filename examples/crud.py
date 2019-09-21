import logging
import os
from pathlib import Path
import sys

from k8s_jobs.manager import JobManager, JobSigner
from k8s_jobs.spec import JobGenerator, YamlFileConfigSource

from kubernetes import config

logging.root.setLevel(logging.NOTSET)


def create(name, signer, *template_kvs):
    path = Path(__file__).parents[0] / f"k8s/{name}.yaml"
    source = YamlFileConfigSource(os.path.abspath(path))
    generator = JobGenerator(source)

    manager = JobManager("default", signer, {name: generator})

    template_args = {
        template_kv.split("=")[0]: template_kv.split("=")[1]
        for template_kv in template_kvs
    }

    job_name = manager.create_job(name, template_args=template_args)
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
        template_kvs = sys.argv[3:]
        create(name, signer, *template_kvs)
    elif action == "delete":
        timeout = int(sys.argv[2])
        delete(signer, timeout)

    jobs = list(manager.fetch_jobs())
    logging.info(f"Jobs: {[job.metadata.name for job in jobs]}")
