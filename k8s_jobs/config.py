from abc import ABC, abstractmethod
from dataclasses import dataclass
import os
import re
from typing import List, Optional
import yaml

from k8s_jobs.manager import JobManager, JobSigner
from k8s_jobs.spec import JobGenerator, YamlFileSpecSource


def env_var_name(name: str) -> str:
    """
    Returns a snake-cased and uppercased version of the given string
    """
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).upper()


@dataclass
class JobDefinition:
    name: str


class JobManagerFactory(ABC):
    @abstractmethod
    def create(self) -> JobManager:
        raise NotImplementedError()


class EnvJobManagerFactory(JobManagerFactory):
    JOB_DEFINITIONS_CONFIG_ROOT = "JOB_DEFINITIONS_CONFIG_ROOT"
    JOB_DEFINITIONS_FILE_NAME = "job_definitions"
    JOB_DEFINITION_PATH_ENV_PREFIX = "JOB_DEFINITION_PATH_"
    JOB_SIGNATURE_ENV_VAR = "JOB_SIGNATURE"
    JOB_NAMESPACE_ENV_VAR = "JOB_NAMESPACE"

    @classmethod
    def from_env(cls, config_root: Optional[str] = None) -> "JobManagerFactory":
        signature = os.environ[cls.JOB_SIGNATURE_ENV_VAR]
        namespace = os.environ[cls.JOB_NAMESPACE_ENV_VAR]

        if not config_root:
            config_root = cls.JOB_DEFINITIONS_CONFIG_ROOT

        job_definitions_file = config_root + "/" + cls.JOB_DEFINITIONS_FILE_NAME
        with open(job_definitions_file, "r") as f:
            job_definitions_dicts = yaml.safe_load(f)

        return cls(
            namespace,
            signature,
            config_root=config_root,
            job_definitions=[JobDefinition(**d) for d in job_definitions_dicts],
        )

    def __init__(
            self, namespace:str,signature: str, config_root: str, job_definitions: List[JobDefinition]
    ):
        self.namespace = namespace
        self.signature = signature
        self.job_definitions = job_definitions
        self.config_root = config_root

    def job_definition_config_path(self, job_definition_name: str) -> str:
        """
        Returns the job definition config path, checking for an environment variable override.

        Users can override the path by setting JOB_DEFINITION_PATH_SNAKE_CASE_NAME_IN_ALL_CAPS
        """
        return os.environ.get(
            f"{self.JOB_DEFINITION_PATH_PREFIX}{env_var_name(job_definition_name)}",
            self.default_path(job_definition_name),
        )

    def job_definition_config_default_path(self, job_definition_name: str) -> str:
        return self.config_root + "/" + job_definition_name

    def create(self) -> JobManager:
        """
        Returns a Job manager based on the config
        """
        return JobManager(
            namespace=self.namespace,
            signer=JobSigner(self.signature),
            job_generators={
                job_definition_name: JobGenerator(
                    YamlFileSpecSource(self.config_path(job_definition_name))
                )
                for job_definition_name in self.job_definition_names
            },
        )
