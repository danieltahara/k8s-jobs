"""
This module contains application config finding/parsing logic
"""
import os
import re
from typing import Dict, List

from k8s_jobs.k8s.job import JobGenerator, YamlFileConfigSource


def env_var_name(name: str) -> str:
    """
    Returns a snake-cased and uppercased version of the given string
    """
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).upper()


class JobDefinitionsConfig:
    JOB_DEFINITION_PATH_PREFIX = "JOB_DEFINITION_PATH_"

    def __init__(self, job_definitions: List[str], config_root: str):
        self.job_definitions = job_definitions
        self.config_root = config_root

    def config_path(self, job_definition_name: str) -> str:
        """
        Returns the job definition config path, checking for an environment variable override.

        Users can override the path by setting JOB_DEFINITION_PATH_SNAKE_CASE_NAME_IN_ALL_CAPS
        """
        return os.environ.get(
            f"{self.JOB_DEFINITION_PATH_PREFIX}{env_var_name(job_definition_name)}",
            self.default_path(job_definition_name),
        )

    def default_path(self, job_definition_name: str) -> str:
        return self.config_root + "/" + job_definition_name

    def make_generators(self) -> Dict[str, JobGenerator]:
        """
        Returns a dict of generators based on the config
        """
        return {
            job_definition_name: JobGenerator(
                YamlFileConfigSource(self.config_path(job_definition_name))
            )
            for job_definition_name in self.job_definition_names
        }
