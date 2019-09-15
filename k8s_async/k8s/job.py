from abc import ABC, abstractmethod
import copy
from datetime import datetime
import logging
import secrets
import threading
import time
from typing import Callable, Dict, Iterator
import yaml

import kubernetes

logger = logging.getLogger(__name__)


class JobConfigSource(ABC):
    @abstractmethod
    def get(self) -> kubernetes.client.V1Job:
        """
        Returns a config dict of the yaml-loaded job spec
        """
        raise NotImplementedError()


class StaticJobConfigSource(JobConfigSource):
    """
    Static config source that returns the initialized dict
    """

    def __init__(self, config: kubernetes.client.V1Job):
        self.config = config

    def get(self) -> kubernetes.client.V1Job:
        return copy.deepcopy(self.config)


class YamlFileConfigSource(JobConfigSource):
    """
    ConfigSource that reads and returns parsed yaml from a file
    """

    def __init__(self, path: str):
        self.path = path

    def get(self) -> kubernetes.client.V1Job:
        with open(self.path, "r") as f:
            return yaml.safe_load(f)


class JobSigner:
    """
    A job signer will add a metadata label to a job to identify it as being created by a particular
    caller, and return a corresponding label selector for such jobs
    """

    LABEL_KEY = "app.kubernetes.io/managed-by"

    def __init__(self, signature: str):
        """
        Args:
            signature: the managed-by label that will be used to identify jobs created by the caller
        """
        self.signature = signature

    def sign(self, job: kubernetes.client.V1Job):
        kubernetes.client.V1ObjectMeta
        if not job.metadata.labels:
            job.metadata.labels = {}
        job.metadata.labels[self.LABEL_KEY] = self.signature

    @property
    def label_selector(self) -> str:
        return f"{self.LABEL_KEY}={self.signature}"


class JobGenerator:
    def __init__(self, config_source: JobConfigSource):
        self.config_source = config_source

    def generate(self) -> kubernetes.client.V1Job:
        """
        Generates a new job spec with a unique name
        """
        config = self.config_source.get()
        config.metadata.name += f"-{secrets.token_hex(24)}"
        return config


class JobManager:
    """
    A JobManager is responsible for managing jobs, creating and deleting completed jobs after a
    grace period.

    Job timeouts are left to the creators of job specs, by setting .spec.activeDeadlineSeconds. Job
    retention is free to be implemented by setting .spec.ttlSecondsAfterFinished, but this is still
    in alpha as of k8s v1.12
    """

    def __init__(
        self,
        client: kubernetes.client,
        namespace: str,
        signer: JobSigner,
        job_generators: Dict[str, JobGenerator],
    ):

        self.client = client
        self.namespace = namespace
        self.signer = signer
        self.job_generators = job_generators

    def create_job(self, job_name: str) -> str:
        """
        Spawn a job for the given job_name
        """
        job = self.job_generators[job_name].generate()
        self.signer.sign(job)
        batch_v1_client = self.client.BatchV1Api()
        response = batch_v1_client.create_namespaced_job(
            namespace=self.namespace, body=job
        )
        logger.debug(response)
        return response.metadata.name

    def delete_job(self, job: kubernetes.client.V1Job):
        batch_v1_client = self.client.BatchV1Api()
        response = batch_v1_client.delete_namespaced_job(
            name=job.metadata.name, namespace=self.namespace
        )
        logger.debug(response)

    def fetch_jobs(self) -> Iterator[kubernetes.client.V1Job]:
        batch_v1_client = self.client.BatchV1Api()
        response = batch_v1_client.list_namespaced_job(
            self.namespace, label_selector=self.signer.label_selector
        )
        yield from response.items
        while response.metadata._continue:
            response = batch_v1_client.list_namespaced_job(
                self.namespace,
                _continue=response.metadata._continue,
            )
            yield from response.items

    def is_old_job(
        self, job: kubernetes.client.V1Job, retention_period_sec: int
    ) -> bool:
        if job.status.completion_time:
            completed_ts = datetime.timestamp(job.status.completion_time)
            if completed_ts + self.retention_period_sec <= time.time():
                return True
        return False

    def delete_old_jobs(self, retention_period_sec: int = 3600):
        for job in self.fetch_jobs():
            if self.is_old_job(job, retention_period_sec):
                self.delete(job)

    def run_background_cleanup(
        self, interval_sec: int = 60, retention_period_sec: int = 3600
    ) -> Callable[[None], None]:
        """
        Starts a background thread that cleans up jobs older than retention_period_sec in a loop,
        waiting interval_sec

        Returns:
            Callable to stop the cleanup loop
        """
        _lock = threading.Lock()
        _stopped = False

        def run():
            while True:
                with self._lock:
                    if self._stopped:
                        return
                try:
                    self.delete_old_jobs(retention_period_sec)
                    time.sleep(interval_sec)
                except Exception as err:
                    logger.warning(err, exc_info=True)

        t = threading.Thread(target=run)
        t.start()

        def stop():
            with _lock:
                nonlocal _stopped
                _stopped = True

        return stop
