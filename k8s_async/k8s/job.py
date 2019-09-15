from abc import ABC, abstractmethod
import logging
import threading
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
        batch_v1_client = self.client.BatchV1Api()
        job = self.generate()
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
        return config

class JobManager:
    """
    A JobManager is responsible for managing jobs, terminating or cleaning up jobs that have
    exceeded a timeout or grace period.
    """

    def __init__(self, client: kubernetes.client, timeout_sec: int,  retention_period_sec: int):
        self.client = client
        self.timeout_sec = timeout_sec
        self.retention_period_sec = retention_period_sec

        self._lock = threading.Lock()
        self._stopped = False
    
    def run_once(self):
        pass

    def run_forever(self):
        while True:
            with self._lock:
                if self._stopped :
                    return
            self.run_once()

    def stop(self):
        with self._lock:
            self._stopped = True
