from datetime import datetime
import logging
import threading
import time
from typing import Callable, Dict, Iterator, Optional, Union

from kubernetes import client

from k8s_jobs.spec import JobGenerator

logger = logging.getLogger(__name__)


class JobSigner:
    """
    A job signer will add a metadata label to a job to identify it as being created by a particular
    caller, and return a corresponding label selector for such jobs
    """

    LABEL_KEY = "app.kubernetes.io/managed-by"
    JOB_DEFINITION_NAME_KEY = "k8s_jobs/job_definition_name"

    def __init__(self, signature: str):
        """
        Args:
            signature: the managed-by label that will be used to identify jobs created by the caller
        """
        self.signature = signature

    def sign(
        self, job: Union[client.V1Job, Dict], job_definition_name: Optional[str] = None
    ):
        """
        Sign sets two labels to make jobs identifiable by the given signer, and filterable by
        job_definition_name
        """
        if isinstance(job, client.V1Job):
            if not job.metadata.labels:
                job.metadata.labels = {}
            labels = job.metadata.labels
        else:
            if "labels" not in job["metadata"]:
                job["metadata"]["labels"] = {}
            labels = job["metadata"]["labels"]

        labels[self.LABEL_KEY] = self.signature
        if job_definition_name:
            labels[self.JOB_DEFINITION_NAME_KEY] = job_definition_name

    def label_selector(self, job_definition_name: Optional[str] = None) -> str:
        """
        Returns the label selector that matches the signature that this signer would add
        """
        selector = f"{self.LABEL_KEY}={self.signature}"
        if job_definition_name:
            selector += f",{self.JOB_DEFINITION_NAME_KEY}={job_definition_name}"
        return selector


class JobManager:
    """
    A JobManager is responsible for managing jobs -- creating, deleting, and responding to status
    requests.

    Job timeouts are left to the creators of job specs, by setting .spec.activeDeadlineSeconds. You
    may also choose to implement Job retention by setting .spec.ttlSecondsAfterFinished, but this is
    still in alpha as of k8s v1.12.
    """

    def __init__(
        self, namespace: str, signer: JobSigner, job_generators: Dict[str, JobGenerator]
    ):

        self.namespace = namespace
        self.signer = signer
        self.job_generators = job_generators

    def create_job(
        self, job_definition_name: str, template_args: Optional[Dict] = None
    ) -> str:
        """
        Spawn a job for the given job_definition_name
        """
        job = self.job_generators[job_definition_name].generate(
            template_args=template_args
        )
        self.signer.sign(job, job_definition_name)
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

    def fetch_jobs(
        self, job_definition_name: Optional[str] = None
    ) -> Iterator[client.V1Job]:
        batch_v1_client = client.BatchV1Api()
        response = batch_v1_client.list_namespaced_job(
            namespace=self.namespace,
            label_selector=self.signer.label_selector(
                job_definition_name=job_definition_name
            ),
        )
        yield from response.items
        while response.metadata._continue:
            response = batch_v1_client.list_namespaced_job(
                namespace=self.namespace, _continue=response.metadata._continue
            )
            yield from response.items

    def job_status(self, job_name: str) -> client.V1JobStatus:
        batch_v1_client = client.BatchV1Api()
        return batch_v1_client.read_namespaced_job_status(
            name=job_name, namespace=self.namespace
        )

    def job_logs(self, job_name: str, limit: Optional[int] = 200) -> str:
        """
        Returns the last limit logs from each pod for the job.

        Each pod's output is delimited by a header and footer line:
            Pod: POD_NAME
            ...
            =======
        """
        core_v1_client = client.CoreV1Api()
        response = core_v1_client.list_namespaced_pod(
            namespace=self.namespace, label_selector=f"job-name={job_name}"
        )
        logs = ""
        for pod in response.items:
            logs += f"Pod: {pod.name}\n"
            logs += core_v1_client.read_namespaced_pod_logs(
                name=pod.metadata.name,
                namespace=self.namespace,
                tail_lines=limit,
                pretty=True,
            )
            logs += "======="
        return logs

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

    def delete_old_jobs(
        self,
        delete_callback: Optional[Callable[[client.V1Job], None]] = None,
        retention_period_sec: int = 3600,
    ):
        """
        Checks the current jobs and deletes any ones that have reached a terminal condition.

        Arguments:
            retention_period_sec: How long ago a job must have reached a terminal (completed,
                failed) state to be considered a candidate for cleanup.
            delete_callback: A callback that is guaranteed to be called at least once before the job
                is permanently deleted. This can be used to persist job history and state. Any
                exceptions raised will therefore block cleanup. Callers are expected to monitor such
                occurences.
        """
        for job in self.fetch_jobs():
            try:
                if self.is_candidate_for_deletion(job, retention_period_sec):
                    try:
                        delete_callback(job)
                    except Exception:
                        logger.warning(f"Error in delete callback", exc_info=True)
                        continue
                    self.delete_job(job)
            except client.rest.ApiException:
                logger.warning(f"Error checking job {job.metadata.name}", exc_info=True)

    def run_background_cleanup(
        self, interval_sec: int = 60, **kwargs
    ) -> Callable[[None], None]:
        """
        Starts a background thread that cleans up jobs older than retention_period_sec in a loop,
        waiting interval_sec

        Arguments:
            interval_sec: time between loops, including the time it takes to perform a check +
                delete.
            **kwargs: Argumetns to delete_old_jobs

        Returns:
            Callable to stop the cleanup loop
        """
        _lock = threading.Lock()
        _stopped = False

        def run():
            while True:
                start = time.time()
                with self._lock:
                    if self._stopped:
                        return
                try:
                    self.delete_old_jobs(**kwargs)
                except Exception as err:
                    logger.warning(err, exc_info=True)
                time.sleep(max(0, interval_sec - (time.time() - start)))

        t = threading.Thread(target=run)
        t.start()

        def stop():
            with _lock:
                nonlocal _stopped
                _stopped = True

        return stop