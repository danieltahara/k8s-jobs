from abc import ABC, abstractmethod
import copy
from datetime import datetime
from io import StringIO
import logging
import secrets
import threading
import time
from typing import Callable, Dict, Iterator, Optional, Union
import yaml

import jinja2
from kubernetes import client

logger = logging.getLogger(__name__)


class JobConfigSource(ABC):
    @abstractmethod
    def get(self, template_args: Optional[Dict] = None) -> Union[client.V1Job, Dict]:
        """
        Returns a V1Job object or a Dict with the same structure
        """
        raise NotImplementedError()


class StaticJobConfigSource(JobConfigSource):
    """
    Static config source that returns the initialized dict. It NOPs against templates
    """

    def __init__(self, config: Union[client.V1Job, Dict]):
        self.config = config

    def get(self, template_args: Optional[Dict] = None) -> Union[client.V1Job, Dict]:
        return copy.deepcopy(self.config)


class YamlFileConfigSource(JobConfigSource):
    """
    ConfigSource that reads and returns parsed yaml from a file, with the given template arguments
    """

    def __init__(self, path: str):
        self.path = path

    def get(self, template_args: Optional[Dict] = None) -> Union[client.V1Job, Dict]:
        jinja2_environment = jinja2.Environment(loader=jinja2.FileSystemLoader("/"))
        rendered = jinja2_environment.get_template(self.path).render(
            template_args or {}
        )
        stream = StringIO(rendered)
        return yaml.safe_load(stream)


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

    # NOTE: This feels a bit awkward that we're plumbing this argument all the way down from the top
    # to the bottom, but I can't think of a clean way to separate config fetching from generation
    # otherwise.
    def generate(
        self, template_args: Optional[Dict] = None
    ) -> Union[client.V1Job, Dict]:
        """
        Generates a new job spec with a unique name
        """
        config = self.config_source.get(template_args=template_args)
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

    # FIXME: Tests
    def create_job(self, job_name: str, template_args: Optional[Dict] = None) -> str:
        """
        Spawn a job for the given job_name
        """
        job = self.job_generators[job_name].generate(template_args=template_args)
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
            # Need deletes to propagate to the created pod(s).
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

    def is_candidate_for_deletion(
        self, job: client.V1Job, retention_period_sec: int
    ) -> bool:
        """
        Is candidate for deletion inspects the job status, and if it is in a terminal state and has
        been in that state more than retention_period_sec, deletes the job
        """
        for condition in job.status.conditions:
            if condition.status != "True":
                continue
            if condition.type not in ["Complete", "Failed"]:
                continue
            last_transition_ts = datetime.timestamp(condition.last_transition_time)
            if last_transition_ts + retention_period_sec > time.time():
                continue
            return True
        return False

    def delete_old_jobs(self, retention_period_sec: int = 3600):
        for job in self.fetch_jobs():
            try:
                if self.is_candidate_for_deletion(job, retention_period_sec):
                    self.delete_job(job)
            except client.rest.ApiException:
                logger.warning(f"Error checking job {job.metadata.name}", exc_info=True)

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
