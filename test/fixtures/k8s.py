import os

import kubernetes
import pytest


@pytest.fixture(scope="session")
def k8s_fixture():
    k8s_config_path = os.environ["TEST_K8S_CONFIG_PATH"]
    if k8s_config_path == kubernetes.config.kube_config.KUBE_CONFIG_DEFAULT_LOCATION:
        raise Exception("Do not use default kubeconfig in itests!")
    kubernetes.config.load_kube_config(k8s_config_path)
