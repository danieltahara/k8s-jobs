from abc import ABC, abstractmethod
from datetime import datetime
import logging
import threading
import time
from typing import Iterator
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


class SimpleConfigSource(JobConfigSource):
    """
    Static config source that returns the initialized dict
    """

    def __init__(self, config: kubernetes.client.V1Job):
        self.config = config

    def get(self) -> kubernetes.client.V1Job:
        return self.config


class YamlFileConfigSource(JobConfigSource):
    """
    ConfigSource that reads and returns parsed yaml from a file
    """

    def __init__(self, path: str):
        self.path = path

    def get(self) -> kubernetes.client.V1Job:
        with open(self.path, "r") as f:
            return yaml.safe_load(f)


class JobCreator:
    @staticmethod
    def generate_name_suffix() -> str:
        return "123"

    def __init__(
        self, client: kubernetes.client, namespace: str, config_source: JobConfigSource
    ):
        """
        Initializes with a kubernetes client and a config dict of the yaml-loaded job spec
        """
        self.client = client
        self.namespace = namespace
        self.config_source = config_source

    def create(self) -> str:
        """
        Creates a new job and returns its name
        """
        job = self.generate()
        batch_v1_client = self.client.BatchV1Api()
        response = batch_v1_client.create_namespaced_job(
            namespace=self.namespace, body=job
        )
        logger.debug(response)
        return response["metadata"]["name"]

    def generate(self) -> kubernetes.client.V1Job:
        """
        Generates a new job spec with a unique name
        """
        config = self.config_source.get()
        config["metadata"]["name"] += self.generate_name_suffix()
        config["metadata"]["labels"]["app.kubernetes.io/managed-by"] = "foobarba"
        return config

# TODO: JobDeleter. Then make JobManager comprise them

class JobManager:
    """
    A JobManager is responsible for managing jobs, cleaning up errored or completed jobs after a
    grace period.

    Job timeouts are left to the creators of job specs, by setting .spec.activeDeadlineSeconds. Job
    retention is free to be implemented by setting .spec.ttlSecondsAfterFinished, but this is still
    in alpha as of k8s v1.12
    """

    def __init__(self, client: kubernetes.client, namespace: str, retention_period_sec: int):
        self.client = client
        self.retention_period_sec = retention_period_sec

        self._lock = threading.Lock()
        self._stopped = False

    def delete_stale_jobs(self):
        for job in self.fetch_jobs():
            if job.status.completion_time:
                completed_ts = datetime.timestamp(job.status.completion_time)
                if completed_ts + self.retention_period_sec <= time.time():
                    self.delete_job(job)

    def delete_job(self, job: kubernetes.client.V1Job):
        batch_v1_client = self.client.BatchV1Api()
        response = batch_v1_client.delete_namespaced_job(
            name=job['metadata']['name'],
            namespace=self.namespace,
        )
        logger.debug(response)

    def fetch_jobs(self) -> Iterator[kubernetes.client.V1Job]:
        batch_v1_client = self.client.BatchV1Api()
        response = batch_v1_client.list_namespaced_job(
            self.namespace,
            label_selector="app.kubernetes.io/managed-by=foobarba",
        )
        yield from response['items']
        while '_continue' in response['metadata']:
            response = batch_v1_client.list_namespaced_job(
                self.namespace,
                _continue=response['metadata']['_continue'],
            )
            yield from response['items']

    def run_forever(self):
        while True:
            with self._lock:
                if self._stopped :
                    return
            try:
                self.delete_stale_jobs()
                time.sleep(5)
            except Exception as err:
                logger.warning(err, exc_info=True)

    def stop(self):
        with self._lock:
            self._stopped = True
