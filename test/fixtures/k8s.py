import os
from unittest import mock
import warnings

import kubernetes
import pytest


@pytest.fixture(scope="session")
def k8s_fixture():
    k8s_config_path = os.environ["TEST_K8S_CONFIG_PATH"]
    if k8s_config_path == kubernetes.config.kube_config.KUBE_CONFIG_DEFAULT_LOCATION:
        raise Exception("Do not use default kubeconfig in itests!")
    warnings.warn("If you need to delete jobs, run `kubectl delete jobs --all`")
    kubernetes.config.load_kube_config(k8s_config_path)


@pytest.fixture
def mock_batch_client():
    with mock.patch("k8s_jobs.manager.client.BatchV1Api") as mock_batch_v1_api:
        yield mock_batch_v1_api.return_value


@pytest.fixture
def mock_core_client():
    with mock.patch("k8s_jobs.manager.client.CoreV1Api") as mock_core_v1_api:
        yield mock_core_v1_api.return_value
