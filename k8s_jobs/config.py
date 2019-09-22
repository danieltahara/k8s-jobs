import os
from dataclasses import dataclass
import logging
from pathlib import Path
import threading
from typing import Dict, List, Optional
import yaml

from k8s_jobs.manager import (
    JobDefinitionsRegister,
    JobManager,
    JobSigner,
    NotFoundException,
)
from k8s_jobs.spec import JobGenerator, StaticJobSpecSource, YamlFileSpecSource

logger = logging.getLogger(__name__)


# def env_var_name(name: str) -> str:
#     """
#     Returns a snake-cased and uppercased version of the given string
#     """
#     s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
#     return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).upper()


@dataclass
class JobDefinition:
    name: str

    # One of the two must be set. Either a) provides an inline job spec/template, or b)
    # provides a path to an external config file that contains the spec.
    spec: Optional[str] = None
    spec_path: Optional[str] = None


class ReloadingJobDefinitionsRegister(JobDefinitionsRegister):
    """
    Concrete JobDefinitionsRegister that checks for config updates and then returns the
    appropriate JobGenerator
    """

    def __init__(self, job_definitions_file: str):
        self._job_definitions_lock = threading.Lock()
        self._job_definitions: List[JobDefinition] = []
        self._job_definitions_file = job_definitions_file
        self._job_definitions_last_modified: float = 0

    def get_generator(self, job_definition_name) -> JobGenerator:
        """
        Raises:
            NotFoundException
        """
        try:
            return self.generators[job_definition_name]
        except KeyError:
            raise NotFoundException(f"Unknown job definition {job_definition_name}")

    def _maybe_reload_job_definitions(self):
        """
        See if the job_definitions config file has been modified. If so, update the
        internal state
        """
        try:
            statbuf = Path(self._job_definitions_file).stat()
        except FileNotFoundError:
            logger.warning(
                f"Could not find job_definitions_file {self._job_definitions_file}"
            )
            return

        with self._job_definitions_lock:
            if statbuf.mtime == self._job_definitions_last_modified:
                return

        # Note that this read is not atomic with the statbuf check, since we don't want
        # to do IO under a lock, hence the CAS below.
        with open(self.job_definitions_file, "r") as f:
            job_definitions_dicts = yaml.safe_load(f)

        job_definitions = [JobDefinition(**d) for d in job_definitions_dicts]

        with self._job_definitions_lock:
            # CAS
            if statbuf.mtime != self._job_definitions_last_modified:
                return
            self._job_definitions = job_definitions
            self._job_definitions_last_modified = statbuf.mtime

    @property
    def generators(self) -> Dict[str, JobGenerator]:
        self._maybe_reload_job_definitions()
        return {
            job_definition.name: JobGenerator(
                YamlFileSpecSource(job_definition.spec_path)
                if job_definition.spec_path
                else StaticJobSpecSource(job_definition.spec)
            )
            for job_definition in self.job_definitions
        }


class JobManagerFactory:
    JOB_SIGNATURE_ENV_VAR = "JOB_SIGNATURE"
    JOB_NAMESPACE_ENV_VAR = "JOB_NAMESPACE"
    JOB_DEFINITIONS_CONFIG_PATH_ENV_VAR = "JOB_DEFINITIONS_CONFIG_PATH"

    @classmethod
    def from_env(cls) -> "JobManagerFactory":
        signature = os.environ[cls.JOB_SIGNATURE_ENV_VAR]
        namespace = os.environ[cls.JOB_NAMESPACE_ENV_VAR]
        job_definitions_file = os.environ[cls.JOB_DEFINITIONS_CONFIG_PATH_ENV_VAR]
        register = ReloadingJobDefinitionsRegister(job_definitions_file)

        return cls(namespace, signature, register)

    def __init__(
        self, namespace: str, signature: str, register: JobDefinitionsRegister
    ):
        self.namespace = namespace
        self.signature = signature
        self.register = register

    def manager(self) -> JobManager:
        """
        Returns a Job manager based on the config
        """
        return JobManager(
            namespace=self.namespace,
            signer=JobSigner(self.signature),
            job_definitions_register=self.register,
        )
