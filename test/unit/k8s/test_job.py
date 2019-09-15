from unittest.mock import ANY, Mock
import yaml

from kubernetes.client import V1Job, V1ObjectMeta
import pytest

from k8s_async.k8s.job import (
    JobGenerator,
    JobManager,
    JobSigner,
    StaticJobConfigSource,
    YamlFileConfigSource,
)


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
    def test_sets_label(self):
        signature = "hehehe"
        signer = JobSigner(signature)
        job = V1Job(metadata=V1ObjectMeta())

        signer.sign(job)

        assert job.metadata.labels[JobSigner.LABEL_KEY] == signature

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


class TestJobManager:
    def test_create_job(self):
        mock_client = Mock()
        mock_batch_client = mock_client.BatchV1Api.return_value
        mock_batch_client.create_namespaced_job.return_value = V1Job(metadata=V1ObjectMeta())
        namespace = "hellomoto"
        g1 = Mock()
        g2 = Mock()
        manager = JobManager(
            mock_client,
            namespace=namespace,
            signer=Mock(),
            job_generators={"g1": g1, "g2": g2},
        )

        manager.create_job("g2")

        g1.assert_not_called()
        g2.generate.assert_called_once()
        mock_batch_client.create_namespaced_job.assert_called_once_with(
            namespace=namespace, body=ANY,
        )

    def test_create_job_unknown(self):
        manager = JobManager(
            Mock(),
            namespace="boohoo",
            signer=Mock(),
            job_generators={},
        )

        with pytest.raises(KeyError):
            manager.create_job("unknown")
