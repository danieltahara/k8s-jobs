from abc import ABC, abstractmethod
import copy
import functools
import secrets
import threading
import yaml
from io import StringIO
from typing import Dict, List, Optional, Union

import jinja2
import jinja2.meta  # Not sure why I need to explicitly specify this...
from kubernetes import client

from k8s_jobs.file_reloader import FileReloader


class JobSpecSource(ABC):
    @abstractmethod
    def get(self, template_args: Optional[Dict] = None) -> Union[client.V1Job, Dict]:
        """
        Returns a V1Job object or a Dict with the same structure
        """
        raise NotImplementedError()

    def template_vars(self) -> List[str]:
        """
        If the spec is a template, return its variables
        """
        raise NotImplementedError()


class StaticSpecSource(JobSpecSource):
    """
    Static config source that returns the initialized dict. It NOPs against templates
    """

    def __init__(self, config: Union[client.V1Job, Dict]):
        self.config = config

    def get(self, template_args: Optional[Dict] = None) -> Union[client.V1Job, Dict]:
        return copy.deepcopy(self.config)

    def template_vars(self) -> List[str]:
        """
        If the spec is a template, return its variables
        """
        return []


class YamlStringSpecSource(JobSpecSource):
    """
    JobSpecSource that returns parsed yaml from a file, with the given template arguments
    """

    def __init__(self, spec: str):
        self.spec = spec

    def get(self, template_args: Optional[Dict] = None) -> Union[client.V1Job, Dict]:
        """
        Returns a strict template, raising exception when expected template args are NOT
        provided.

        Note that the converse is not true. You are free to provide unnecessary template
        args.

        Raises:
            jinja2.exceptions.UndefinedError
        """
        rendered = jinja2.Template(self.spec, undefined=jinja2.StrictUndefined).render(
            template_args or {}
        )
        stream = StringIO(rendered)
        return yaml.safe_load(stream)

    def template_vars(self) -> List[str]:
        """
        If the spec is a template, return its variables
        """
        env = jinja2.Environment()
        ast = env.parse(self.spec)
        return list(jinja2.meta.find_undeclared_variables(ast))


class YamlFileSpecSource(JobSpecSource):
    """
    SpecSource that reads and returns parsed yaml from a file, with the given template arguments
    """

    def __init__(self, path: str):
        self._reloader = FileReloader(path)
        self._lock = threading.Lock()
        self._maybe_reload()

    def _maybe_reload(self):
        update = self._reloader.maybe_reload()

        try:
            reader = next(update)
        except StopIteration:
            return

        spec = reader.read()
        string_spec_source = YamlStringSpecSource(spec)

        update.send(functools.partial(self._set_string_spec_source, string_spec_source))

    def _get_string_spec_source(self) -> YamlStringSpecSource:
        with self._lock:
            return self._string_spec_source

    def _set_string_spec_source(self, string_spec_source: YamlStringSpecSource):
        with self._lock:
            self._string_spec_source = string_spec_source

    def get(self, template_args: Optional[Dict] = None) -> Union[client.V1Job, Dict]:
        self._maybe_reload()
        return self._get_string_spec_source().get(template_args)

    def template_vars(self) -> List[str]:
        return self._get_string_spec_source().template_vars()


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

    def _get_string_spec_source(self) -> YamlStringSpecSource:
        core_v1_client = client.CoreV1Api()
        # Unfortunately, we have to fetch the whole config map object in order to check
        # the metadata.resource_version, so we save nothing by trying to only reload
        # when that value has changed (as yaml parsing is lazy, after templating).
        v1_config_map = core_v1_client.read_namespaced_config_map(
            name=self.name, namespace=self.namespace
        )
        return YamlStringSpecSource(v1_config_map.data[self.name])

    def get(self, template_args: Optional[Dict] = None) -> Union[client.V1Job, Dict]:
        return self._get_string_spec_source().get(template_args)

    def template_vars(self) -> List[str]:
        return self._get_string_spec_source().template_vars()


class JobGenerator:
    # I'm not sure the exact behavior when trying to create a job with a name that
    # already exists. This should give low enough collision odds anyway, as long as job
    # retention isn't too long.
    SUFFIX_BYTES = 12
    MAX_LEN = 63 - 1 - 2 * SUFFIX_BYTES

    def __init__(self, spec_source: JobSpecSource):
        self.spec_source = spec_source

    # NOTE: This feels a bit awkward that we're plumbing this argument all the way down from the top
    # to the bottom, but I can't think of a clean way to separate config fetching from generation
    # otherwise.
    def generate(
        self, template_args: Optional[Dict] = None
    ) -> Union[client.V1Job, Dict]:
        """
        Generates a new job spec with a unique name
        """
        config = self.spec_source.get(template_args=template_args)
        if isinstance(config, client.V1Job):
            config.metadata.name = f"{config.metadata.name[:self.MAX_LEN]}-{secrets.token_hex(self.SUFFIX_BYTES)}"
        else:
            config["metadata"][
                "name"
            ] = f"{config['metadata']['name'][:self.MAX_LEN]}-{secrets.token_hex(self.SUFFIX_BYTES)}"
        return config

    def template_vars(self) -> List[str]:
        return self.spec_source.template_vars()
