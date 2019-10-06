from dataclasses import dataclass
import functools
import logging
import os
import threading
from typing import Dict, Optional
import yaml

from k8s_jobs.exceptions import NotFoundException, remaps_exception
from k8s_jobs.file_reloader import FileReloader
from k8s_jobs.manager import JobDefinitionsRegister, JobManager, JobSigner
from k8s_jobs.spec import (
    ConfigMapSpecSource,
    JobGenerator,
    JobSpecSource,
    YamlStringSpecSource,
    YamlFileSpecSource,
)

logger = logging.getLogger(__name__)


@dataclass
class JobDefinition:
    name: str

    # Inline (non templated) job-spec
    spec: Optional[str] = None
    # Path to (potentially templated) job spec file
    spec_path: Optional[str] = None
    # ConfigMap + NS for a (potentially templated) job spec. The confg map name and the
    # key containing the spec are required to be the same.
    spec_config_map_name: Optional[str] = None
    spec_config_map_namespace: Optional[str] = None

    def spec_source(self) -> JobSpecSource:
        if self.spec:
            return YamlStringSpecSource(self.spec)
        elif self.spec_path:
            return YamlFileSpecSource(self.spec_path)
        else:
            return ConfigMapSpecSource(
                self.spec_config_map_name, self.spec_config_map_namespace
            )


class ReloadingJobDefinitionsRegister(JobDefinitionsRegister):
    """
    Concrete JobDefinitionsRegister that checks for config updates and then returns the
    appropriate JobGenerator
    """

    def __init__(self, reloader: FileReloader):
        self._reloader = reloader
        self._lock = threading.Lock()
        self._maybe_reload()

    @remaps_exception({KeyError: NotFoundException})
    def get_generator(self, job_definition_name) -> JobGenerator:
        """
        Raises:
            NotFoundException
        """
        return self.generators[job_definition_name]

    def _maybe_reload(self):
        """
        See if the job_definitions config file has been modified. If so, update the
        internal state
        """
        update = self._reloader.maybe_reload()

        try:
            reader = next(update)
        except StopIteration:
            return

        job_definitions_dicts = yaml.safe_load(reader)
        job_definitions = [JobDefinition(**d) for d in job_definitions_dicts]
        generators = {
            job_definition.name: job_definition.spec_source()
            for job_definition in job_definitions
        }

        update.send(functools.partial(self._set_generators, generators))

    def _set_generators(self, generators: Dict[str, JobGenerator]):
        with self._lock:
            self._generators = generators

    @property
    def generators(self) -> Dict[str, JobGenerator]:
        self._maybe_reload()
        with self._lock:
            return self._generators


class JobManagerFactory:
    JOB_SIGNATURE_ENV_VAR = "JOB_SIGNATURE"
    JOB_NAMESPACE_ENV_VAR = "JOB_NAMESPACE"
    JOB_DEFINITIONS_CONFIG_PATH_ENV_VAR = "JOB_DEFINITIONS_CONFIG_PATH"

    @classmethod
    def from_env(cls) -> "JobManagerFactory":
        """
        Creates a JobManagerFactory that will auto-reload any changes to
        job_definitions, from environment variables
        """
        signature = os.environ[cls.JOB_SIGNATURE_ENV_VAR]
        namespace = os.environ[cls.JOB_NAMESPACE_ENV_VAR]
        job_definitions_file_path = os.environ[cls.JOB_DEFINITIONS_CONFIG_PATH_ENV_VAR]
        return cls.from_vars(signature, namespace, job_definitions_file_path)

    @classmethod
    def from_vars(
        cls, signature: str, namespace: str, job_definitions_file_path: str
    ) -> "JobManagerFactory":
        """
        Creates a JobManagerFactory that will auto-reload any changes to
        job_definitions, from the given variables
        """
        register = ReloadingJobDefinitionsRegister(
            FileReloader(job_definitions_file_path)
        )

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
            register=self.register,
        )
