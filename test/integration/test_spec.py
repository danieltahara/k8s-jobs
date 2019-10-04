import yaml

from kubernetes import client
import pytest

from k8s_jobs.spec import ConfigMapSpecSource
from test.fixtures.examples import EXAMPLES_ROOT

pytestmark = [pytest.mark.k8s_itest, pytest.mark.usefixtures("k8s_fixture")]


class TestSpecSource:
    def test_config_map_spec_source(self):
        with open(f"{EXAMPLES_ROOT}/configmap-helloworld.yaml") as f:
            config_map = yaml.safe_load(f)
        name = config_map["metadata"]["name"]
        job = yaml.safe_load(config_map["data"][name])
        core_v1_client = client.CoreV1Api()
        try:
            core_v1_client.create_namespaced_config_map(
                namespace="default", body=config_map
            )
            spec_source = ConfigMapSpecSource(name, "default")

            spec = spec_source.get()

            assert spec == job
        finally:
            # Clean up after ourselves
            core_v1_client.delete_namespaced_config_map(namespace="default", name=name)
