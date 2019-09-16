from abc import ABC, abstractmethod
import copy
from datetime import datetime
import logging
import secrets
import threading
import time
from typing import Callable, Dict, Iterator, Union
import yaml

from kubernetes import client

logger = logging.getLogger(__name__)


class JobConfigSource(ABC):
    @abstractmethod
    def get(self) -> Union[client.V1Job, Dict]:
        """
        Returns a V1Job object or a Dict with the same structure
        """
        raise NotImplementedError()


class StaticJobConfigSource(JobConfigSource):
    """
    Static config source that returns the initialized dict
    """

    def __init__(self, config: Union[client.V1Job, Dict]):
        self.config = config

    def get(self) -> Union[client.V1Job, Dict]:
        return copy.deepcopy(self.config)


class YamlFileConfigSource(JobConfigSource):
    """
    ConfigSource that reads and returns parsed yaml from a file
    """

    def __init__(self, path: str):
        self.path = path

    def get(self) -> Union[client.V1Job, Dict]:
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

    def sign(self, job: Union[client.V1Job, Dict]):
        if isinstance(job, client.V1Job):
            if not job.metadata.labels:
                job.metadata.labels = {}
            job.metadata.labels[self.LABEL_KEY] = self.signature
        else:
            if "labels" not in job["metadata"]:
                job["metadata"]["labels"] = {}
            job["metadata"]["labels"][self.LABEL_KEY] = self.signature

    @property
    def label_selector(self) -> str:
        return f"{self.LABEL_KEY}={self.signature}"


class JobGenerator:
    def __init__(self, config_source: JobConfigSource):
        self.config_source = config_source

    def generate(self) -> Union[client.V1Job, Dict]:
        """
        Generates a new job spec with a unique name
        """
        config = self.config_source.get()
        if isinstance(config, client.V1Job):
            config.metadata.name += f"-{secrets.token_hex(24)}"
        else:
            config["metadata"]["name"] += f"-{secrets.token_hex(24)}"
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
        self, namespace: str, signer: JobSigner, job_generators: Dict[str, JobGenerator]
    ):

        self.namespace = namespace
        self.signer = signer
        self.job_generators = job_generators

    def create_job(self, job_name: str) -> str:
        """
        Spawn a job for the given job_name
        """
        job = self.job_generators[job_name].generate()
        self.signer.sign(job)
        batch_v1_client = client.BatchV1Api()
        response = batch_v1_client.create_namespaced_job(
            namespace=self.namespace, body=job
        )
        logger.debug(response)
        return response.metadata.name

    def delete_job(self, job: client.V1Job):
        batch_v1_client = client.BatchV1Api()
        response = batch_v1_client.delete_namespaced_job(
            name=job.metadata.name,
            namespace=self.namespace,
            # FIXME: Test
            # Need deletes to propagate to the created pod.
            body=client.V1DeleteOptions(propagation_policy="Foreground"),
        )
        logger.debug(response)

    def fetch_jobs(self) -> Iterator[client.V1Job]:
        batch_v1_client = client.BatchV1Api()
        response = batch_v1_client.list_namespaced_job(
            namespace=self.namespace, label_selector=self.signer.label_selector
        )
        yield from response.items
        while response.metadata._continue:
            response = batch_v1_client.list_namespaced_job(
                namespace=self.namespace, _continue=response.metadata._continue
            )
            yield from response.items

    # FIXME: Completed ts is not set for failed jobs
    def is_old_job(self, job: client.V1Job, retention_period_sec: int) -> bool:
        if job.status.completion_time:
            completed_ts = datetime.timestamp(job.status.completion_time)
            if completed_ts + retention_period_sec <= time.time():
                return True
        return False

    def delete_old_jobs(self, retention_period_sec: int = 3600):
        for job in self.fetch_jobs():
            if self.is_old_job(job, retention_period_sec):
                self.delete_job(job)

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
                except Exception as err:
                    logger.warning(err, exc_info=True)
                time.sleep(interval_sec)

        t = threading.Thread(target=run)
        t.start()

        def stop():
            with _lock:
                nonlocal _stopped
                _stopped = True

        return stop
