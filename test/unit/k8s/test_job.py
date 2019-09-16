from datetime import datetime, timedelta
from unittest.mock import ANY, Mock, patch
import yaml

from kubernetes.client import V1Job, V1JobList, V1JobStatus, V1ListMeta, V1ObjectMeta
import pytest

from k8s_async.k8s.job import (
    JobGenerator,
    JobManager,
    JobSigner,
    StaticJobConfigSource,
    YamlFileConfigSource,
)


@pytest.fixture
def MockBatchV1Api():
    with patch("k8s_async.k8s.job.client.BatchV1Api") as mock_batch_v1_api:
        yield mock_batch_v1_api


class TestConfigSource:
    def test_yaml_config_source_reloads(self, request, tmp_path):
        d1 = {"foo": "bar"}
        d2 = {"biz": "buzz"}
        tmp_file_name = tmp_path / request.node.name

        with open(tmp_file_name, "w+") as f:
            yaml.dump(d1, f)
        c = YamlFileConfigSource(tmp_file_name)
        assert d1 == c.get()

        with open(tmp_file_name, "w+") as f:
            yaml.dump(d2, f)
        assert d2 == c.get()


class TestJobSignatureGenerator:
    def test_sets_label_job(self):
        signature = "hehehe"
        signer = JobSigner(signature)
        job = V1Job(metadata=V1ObjectMeta())

        signer.sign(job)

        assert (
            job.metadata.labels[JobSigner.LABEL_KEY] == signature
        ), "Metadata label not set"

    def test_sets_label_dict(self):
        signature = "hehehe"
        signer = JobSigner(signature)
        job = {"metadata": {}}

        signer.sign(job)

        assert (
            job["metadata"]["labels"][JobSigner.LABEL_KEY] == signature
        ), "Metadata label not set"

    def test_label_selector(self):
        signature = "woahhhh"
        signer = JobSigner(signature)

        assert signer.label_selector == f"{JobSigner.LABEL_KEY}={signature}"


class TestJobGenerator:
    def test_unique_names(self):
        generator = JobGenerator(
            StaticJobConfigSource(
                V1Job(metadata=V1ObjectMeta(name="iloveyouabushelandapeck"))
            )
        )

        j1 = generator.generate()
        j2 = generator.generate()

        assert (
            j1.metadata.name != j2.metadata.name
        ), "Each generated job must have a unique name"

    def test_generate_with_dict_config(self):
        job = V1Job(metadata=V1ObjectMeta(name="iloveyouabushelandapeck"))
        generator = JobGenerator(StaticJobConfigSource(job.to_dict()))

        j = generator.generate()
        assert (
            j["metadata"]["name"] != job.metadata.name
        ), "Should have mutated job name"


class TestJobManager:
    def test_create_job(self, MockBatchV1Api):
        mock_batch_client = MockBatchV1Api.return_value
        mock_batch_client.create_namespaced_job.return_value = V1Job(
            metadata=V1ObjectMeta()
        )
        namespace = "hellomoto"
        g1 = Mock()
        g2 = Mock()
        manager = JobManager(
            namespace=namespace, signer=Mock(), job_generators={"g1": g1, "g2": g2}
        )

        manager.create_job("g2")

        g1.assert_not_called()
        g2.generate.assert_called_once()
        mock_batch_client.create_namespaced_job.assert_called_once_with(
            namespace=namespace, body=ANY
        )

    def test_create_job_unknown(self):
        manager = JobManager(namespace="boohoo", signer=Mock(), job_generators={})

        with pytest.raises(KeyError):
            manager.create_job("unknown")

    def test_is_old_job(self):
        manager = JobManager(namespace="fake", signer=Mock(), job_generators={})

        job = V1Job(status=V1JobStatus())
        assert not manager.is_old_job(job, 100)

        now = datetime.now()
        job = V1Job(status=V1JobStatus(completion_time=now))
        assert not manager.is_old_job(job, 100)

        before = now - timedelta(seconds=101)
        job = V1Job(status=V1JobStatus(completion_time=before))
        assert manager.is_old_job(job, 100)

    def test_fetch_jobs(self, MockBatchV1Api):
        mock_batch_client = MockBatchV1Api.return_value
        mock_batch_client.list_namespaced_job.return_value = V1JobList(
            items=[1], metadata=V1ListMeta()
        )
        namespace = "hellomoto"
        signer = JobSigner("foo")
        manager = JobManager(namespace=namespace, signer=signer, job_generators={})

        assert len(list(manager.fetch_jobs())) == 1
        mock_batch_client.list_namespaced_job.assert_called_once_with(
            namespace=namespace, label_selector=signer.label_selector
        )

    def test_fetch_jobs_continue(self, MockBatchV1Api):
        mock_batch_client = MockBatchV1Api.return_value
        _continue = "xyz"
        mock_batch_client.list_namespaced_job.side_effect = [
            V1JobList(items=[1], metadata=V1ListMeta(_continue=_continue)),
            V1JobList(items=[2], metadata=V1ListMeta()),
        ]
        namespace = "blech"
        manager = JobManager(namespace=namespace, signer=Mock(), job_generators={})

        assert len(list(manager.fetch_jobs())) == 2
        assert mock_batch_client.list_namespaced_job.call_count == 2
        mock_batch_client.list_namespaced_job.assert_called_with(
            namespace=namespace, _continue=_continue
        )
