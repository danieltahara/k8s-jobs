import logging
from pathlib import Path
import sys

from k8s_async.k8s.job import JobManager, JobGenerator, JobSigner, YamlFileConfigSource

from kubernetes import config

if __name__ == '__main__':
    name = sys.argv[1]
    source = YamlFileConfigSource(f"{Path(__file__).parents[0]}/k8s/{name}.yaml")
    generator = JobGenerator(source)
    signer = JobSigner("simple-example")

    config.load_kube_config()

    manager = JobManager("default", signer, {name: generator})
    job_name = manager.create_job(name)
    logging.info(f"Created job {job_name}")

    jobs = list(manager.fetch_jobs())
    logging.info(f"Jobs: {jobs}")
