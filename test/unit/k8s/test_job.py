from unittest.mock import Mock
import yaml

from kubernetes.client import V1Job, V1ObjectMeta

from k8s_async.k8s.job import JobGenerator, JobSigner, StaticJobConfigSource, YamlFileConfigSource

class TestConfigSource:
    def test_yaml_config_source_reloads(self, request, tmp_path):
        d1 = {"foo": "bar"}
        d2 = {"biz": "buzz"}
        tmp_file_name = tmp_path + request.node.name
        yaml.dump(d1, tmp_file_name)

        c = YamlFileConfigSource(tmp_file_name)
        assert d1 == c.get()

        yaml.dump(d2, tmp_file_name)
        assert d2 == c.get()

class TestJobSignatureGenerator:
    def test_sets_label(self):
        signature = "hehehe"
        signer = JobSigner(signature)
        job = V1Job()

        signer.sign(job)

        assert job.metadata.labels[JobSigner.LABEL_KEY] == signature

    def test_label_selector(self):
        signature = "woahhhh"
        signer = JobSigner(signature)

        assert signer.label_selector == f"{JobSigner.LABEL_KEY}={signature}"

class TestJobGenerator:
    def test_unique_names(self):
        generator = JobGenerator(StaticJobConfigSource(
        V1Job(
            metadata=V1ObjectMeta(name="iloveyouabushelandapeck")
        )
        ))

        j1 = generator.generate()
        j2 = generator.generate()

        assert j1.metadata.name != j2.metadata.name, "Each generated job must have a unique name"

class TestJobManager:
    def test_create_job(self):
        mock_client = Mock()
        namespace = "hellomoto"
        g1 = Mock()
        g2 = Mock()
        manager = JobManager(
                mock_client,
                namespace="hellomoto",
                signer=Mock(),
                job_generators={"g1": g1, "g2": g2},
        )

        # Test it hits batch create
        # Test it picks the right generator

        assert False
