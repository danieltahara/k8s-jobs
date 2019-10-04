from abc import ABC, abstractmethod
import copy
from io import StringIO
import secrets
from typing import Dict, Optional, Union
import yaml

import jinja2
from kubernetes import client


class JobSpecSource(ABC):
    @abstractmethod
    def get(self, template_args: Optional[Dict] = None) -> Union[client.V1Job, Dict]:
        """
        Returns a V1Job object or a Dict with the same structure
        """
        raise NotImplementedError()


# TODO: Provide a templateable version that just takes a string.
class StaticJobSpecSource(JobSpecSource):
    """
    Static config source that returns the initialized dict. It NOPs against templates
    """

    def __init__(self, config: Union[client.V1Job, Dict]):
        self.config = config

    def get(self, template_args: Optional[Dict] = None) -> Union[client.V1Job, Dict]:
        return copy.deepcopy(self.config)


class YamlFileSpecSource(JobSpecSource):
    """
    SpecSource that reads and returns parsed yaml from a file, with the given template arguments
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


class ConfigMapSpecSource(JobSpecSource):
    """
    Config source that reads a config map of the given name and returns the (templated)
    job spec stored in the data field under a key of the same name. For example:

        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: helloworld
        data:
          helloworld: |
            apiVersion: batch/v1
            kind: Job
            ...
    """

    def __init__(self, name: str, namespace: str):
        self.name = name
        self.namespace = namespace

    def get(self, template_args: Optional[Dict] = None) -> Union[client.V1Job, Dict]:
        core_v1_client = client.CoreV1Api()
        v1_config_map = core_v1_client.read_namespaced_config_map(
            name=self.name, namespace=self.namespace
        )
        rendered = jinja2.Template(v1_config_map.data[self.name]).render(
            template_args or {}
        )
        stream = StringIO(rendered)
        return yaml.safe_load(stream)


class JobGenerator:
    # I'm not sure the exact behavior when trying to create a job with a name that
    # already exists. This should give low enough collision odds anyway, as long as job
    # retention isn't too long.
    SUFFIX_BYTES = 12
    MAX_LEN = 63 - 1 - 2 * SUFFIX_BYTES

    def __init__(self, config_source: JobSpecSource):
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
            config.metadata.name = f"{config.metadata.name[:self.MAX_LEN]}-{secrets.token_hex(self.SUFFIX_BYTES)}"
        else:
            config["metadata"][
                "name"
            ] = f"{config['metadata']['name'][:self.MAX_LEN]}-{secrets.token_hex(self.SUFFIX_BYTES)}"
        return config
