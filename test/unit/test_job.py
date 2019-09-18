from datetime import datetime, timedelta
from unittest.mock import ANY, Mock, patch
import yaml

from kubernetes.client import (
    V1DeleteOptions,
    V1Job,
    V1JobCondition,
    V1JobList,
    V1JobStatus,
    V1ListMeta,
    V1ObjectMeta,
)
from kubernetes.client.rest import ApiException
import pytest

from k8s_jobs.job import (
    JobGenerator,
    JobManager,
    JobSigner,
    StaticJobConfigSource,
    YamlFileConfigSource,
)


@pytest.fixture
def mock_batch_client():
    with patch("k8s_jobs.k8s.job.client.BatchV1Api") as mock_batch_v1_api:
        yield mock_batch_v1_api.return_value


class TestConfigSource:
    def test_yaml_config_source_reloads(self, request, tmp_path):
        d1 = {"foo": "bar"}
        d2 = {"biz": "buzz"}
        tmp_file_name = tmp_path / request.node.name

        with open(tmp_file_name, "w+") as f:
            yaml.dump(d1, f)
        c = YamlFileConfigSource(str(tmp_file_name))
        assert d1 == c.get()

        with open(tmp_file_name, "w+") as f:
            yaml.dump(d2, f)
        assert d2 == c.get()

    def test_yaml_config_source_templates(self, request, tmp_path):
        jinja_d = {"biz": "{{ buzz }}"}
        tmp_file_name = tmp_path / request.node.name
        with open(tmp_file_name, "w+") as f:
            yaml.dump(jinja_d, f)

        c = YamlFileConfigSource(str(tmp_file_name))

        assert {"biz": "foo"} == c.get(template_args={"buzz": "foo"})


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

    def test_generate_with_template_args(self):
        mock_config_source = Mock()
        mock_config_source.get.return_value = V1Job(
            metadata=V1ObjectMeta(name="anotherone")
        )
        generator = JobGenerator(mock_config_source)
        template_args = {"foo": "bar"}

        generator.generate(template_args=template_args)

        mock_config_source.get.assert_called_once_with(template_args=template_args)


class TestJobManager:
    def test_create_job(self, mock_batch_client):
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

    def test_create_job_with_template(self, mock_batch_client):
        mock_batch_client.create_namespaced_job.return_value = V1Job(
            metadata=V1ObjectMeta()
        )
        job_name = "job"
        mock_generator = Mock()
        manager = JobManager(
            namespace="geerick",
            signer=Mock(),
            job_generators={job_name: mock_generator},
        )
        template_args = {"dummy": "template"}

        manager.create_job(job_name, template_args=template_args)

        mock_generator.generate.assert_called_once_with(template_args=template_args)

    def test_delete_job(self, mock_batch_client):
        namespace = "whee"
        name = "jobname"
        manager = JobManager(namespace=namespace, signer=Mock(), job_generators={})

        manager.delete_job(V1Job(metadata=V1ObjectMeta(name=name)))

        mock_batch_client.delete_namespaced_job.assert_called_once_with(
            name=name,
            namespace=namespace,
            body=V1DeleteOptions(propagation_policy="Foreground"),
        )

    def test_is_candidate_for_deletion(self):
        manager = JobManager(namespace="fake", signer=Mock(), job_generators={})
        now = datetime.now()
        before = now - timedelta(seconds=101)

        job = V1Job(status=V1JobStatus(conditions=[]))
        assert not manager.is_candidate_for_deletion(job, 100)

        job = V1Job(status=V1JobStatus(conditions=[], completion_time=now))
        assert not manager.is_candidate_for_deletion(job, 100)

        job = V1Job(
            status=V1JobStatus(
                conditions=[
                    V1JobCondition(
                        last_transition_time=now, status="True", type="Complete"
                    )
                ]
            )
        )
        assert not manager.is_candidate_for_deletion(
            job, 100
        ), "A recently completed job should not be deleted"

        job = V1Job(
            status=V1JobStatus(
                conditions=[
                    V1JobCondition(
                        last_transition_time=before, status="True", type="Complete"
                    )
                ]
            )
        )
        assert manager.is_candidate_for_deletion(
            job, 100
        ), "Job that completed a while ago should be deleted"

        job = V1Job(
            status=V1JobStatus(
                conditions=[
                    V1JobCondition(
                        last_transition_time=before, status="False", type="Complete"
                    )
                ]
            )
        )
        assert not manager.is_candidate_for_deletion(
            job, 100
        ), "False job status conditions should be ignored"

        job = V1Job(
            status=V1JobStatus(
                conditions=[
                    V1JobCondition(
                        last_transition_time=before, status="True", type="Failed"
                    )
                ]
            )
        )
        assert manager.is_candidate_for_deletion(
            job, 100
        ), "Job that failed a while ago should be deleted"

    def test_delete_old_jobs_error(self, mock_batch_client):
        manager = JobManager(namespace="harhar", signer=Mock(), job_generators={})

        with patch.object(
            manager, "delete_job", side_effect=[ApiException, None]
        ) as mock_delete_job:
            with patch.object(manager, "fetch_jobs", return_value=[Mock(), Mock()]):
                with patch.object(
                    manager, "is_candidate_for_deletion", return_value=True
                ):
                    # Should not raise
                    manager.delete_old_jobs()

                assert mock_delete_job.call_count == 2

    def test_fetch_jobs(self, mock_batch_client):
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

    def test_fetch_jobs_continue(self, mock_batch_client):
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
