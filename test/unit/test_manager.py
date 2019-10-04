from datetime import datetime, timedelta
from unittest.mock import ANY, Mock, patch

from kubernetes.client import (
    V1DeleteOptions,
    V1Job,
    V1JobCondition,
    V1JobList,
    V1JobStatus,
    V1ListMeta,
    V1ObjectMeta,
    V1Pod,
)
from kubernetes.client.rest import ApiException
import pytest

from k8s_jobs.manager import (
    JobManager,
    JobSigner,
    NotFoundException,
    StaticJobDefinitionsRegister,
)


@pytest.fixture
def mock_batch_client():
    with patch("k8s_jobs.manager.client.BatchV1Api") as mock_batch_v1_api:
        yield mock_batch_v1_api.return_value


@pytest.fixture
def mock_core_client():
    with patch("k8s_jobs.manager.client.CoreV1Api") as mock_core_v1_api:
        yield mock_core_v1_api.return_value


class TestJobSignatureGenerator:
    def test_sets_label_job(self):
        signature = "hehehe"
        signer = JobSigner(signature)
        job = V1Job(metadata=V1ObjectMeta())

        signer.sign(job)

        assert (
            job.metadata.labels[JobSigner.LABEL_KEY] == signature
        ), "Metadata label not set"

        job_definition_name = "funfun"
        signer.sign(job, job_definition_name)
        assert (
            job.metadata.labels[JobSigner.JOB_DEFINITION_NAME_KEY]
            == job_definition_name
        ), "Job Definition label not set"

    def test_sets_label_dict(self):
        signature = "hehehe"
        signer = JobSigner(signature)
        job = {"metadata": {}}

        signer.sign(job)

        assert (
            job["metadata"]["labels"][JobSigner.LABEL_KEY] == signature
        ), "Metadata label not set"

        job_definition_name = "tbirdaway"
        signer.sign(job, job_definition_name)
        assert (
            job["metadata"]["labels"][JobSigner.JOB_DEFINITION_NAME_KEY]
            == job_definition_name
        ), "Job Definition label not set"

    def test_label_selector(self):
        signature = "woahhhh"
        signer = JobSigner(signature)

        assert signer.label_selector() == f"{JobSigner.LABEL_KEY}={signature}"

        job_definition_name = "jdphd"
        assert signer.label_selector(job_definition_name).split(",") == [
            signer.label_selector(),
            f"{JobSigner.JOB_DEFINITION_NAME_KEY}={job_definition_name}",
        ]


class TestJobManager:
    def test_create_job(self, mock_batch_client):
        mock_batch_client.create_namespaced_job.return_value = V1Job(
            metadata=V1ObjectMeta()
        )
        namespace = "hellomoto"
        g1 = Mock()
        g2 = Mock()
        manager = JobManager(
            namespace=namespace,
            signer=Mock(),
            register=StaticJobDefinitionsRegister({"g1": g1, "g2": g2}),
        )

        manager.create_job("g2")

        g1.assert_not_called()
        g2.generate.assert_called_once()
        mock_batch_client.create_namespaced_job.assert_called_once_with(
            namespace=namespace, body=ANY
        )

    def test_create_job_unknown(self):
        manager = JobManager(
            namespace="boohoo", signer=Mock(), register=StaticJobDefinitionsRegister()
        )

        with pytest.raises(NotFoundException):
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
            register=StaticJobDefinitionsRegister({job_name: mock_generator}),
        )
        template_args = {"dummy": "template"}

        manager.create_job(job_name, template_args=template_args)

        mock_generator.generate.assert_called_once_with(template_args=template_args)

    def test_delete_job(self, mock_batch_client):
        namespace = "whee"
        name = "jobname"
        manager = JobManager(
            namespace=namespace, signer=Mock(), register=StaticJobDefinitionsRegister()
        )

        manager.delete_job(V1Job(metadata=V1ObjectMeta(name=name)))

        mock_batch_client.delete_namespaced_job.assert_called_once_with(
            name=name,
            namespace=namespace,
            body=V1DeleteOptions(propagation_policy="Foreground"),
        )

    def test_is_candidate_for_deletion(self):
        manager = JobManager(
            namespace="fake", signer=Mock(), register=StaticJobDefinitionsRegister()
        )
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
        manager = JobManager(
            namespace="harhar", signer=Mock(), register=StaticJobDefinitionsRegister()
        )

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

    def test_delete_old_jobs_callback(self, mock_batch_client):
        manager = JobManager(
            namespace="owahhh", signer=Mock(), register=StaticJobDefinitionsRegister()
        )
        with patch.object(manager, "delete_job", return_value=None):
            with patch.object(manager, "fetch_jobs", return_value=[Mock(), Mock()]):
                with patch.object(
                    manager, "is_candidate_for_deletion", return_value=True
                ):
                    mock_callback = Mock()

                    manager.delete_old_jobs(delete_callback=mock_callback)

                    assert mock_callback.call_count == 2

    def test_fetch_jobs(self, mock_batch_client):
        mock_batch_client.list_namespaced_job.return_value = V1JobList(
            items=[V1Job(metadata=V1ObjectMeta(name="1"))], metadata=V1ListMeta()
        )
        namespace = "hellomoto"
        signer = JobSigner("foo")
        manager = JobManager(
            namespace=namespace, signer=signer, register=StaticJobDefinitionsRegister()
        )

        assert len(list(manager.fetch_jobs())) == 1
        mock_batch_client.list_namespaced_job.assert_called_once_with(
            namespace=namespace, label_selector=signer.label_selector()
        )

    def test_fetch_jobs_continue(self, mock_batch_client):
        _continue = "xyz"
        mock_batch_client.list_namespaced_job.side_effect = [
            V1JobList(
                items=[V1Job(metadata=V1ObjectMeta(name="1"))],
                metadata=V1ListMeta(_continue=_continue),
            ),
            V1JobList(
                items=[V1Job(metadata=V1ObjectMeta(name="2"))], metadata=V1ListMeta()
            ),
        ]
        namespace = "blech"
        manager = JobManager(
            namespace=namespace, signer=Mock(), register=StaticJobDefinitionsRegister()
        )

        assert len(list(manager.fetch_jobs())) == 2
        assert mock_batch_client.list_namespaced_job.call_count == 2
        mock_batch_client.list_namespaced_job.assert_called_with(
            namespace=namespace, _continue=_continue
        )

    def test_fetch_jobs_job_definition_name(self, mock_batch_client):
        namespace = "phd"
        signer = JobSigner("school")
        manager = JobManager(
            namespace=namespace, signer=signer, register=StaticJobDefinitionsRegister()
        )
        job_definition_name = "jd"
        mock_batch_client.list_namespaced_job.return_value = V1JobList(
            items=[], metadata=V1ListMeta()
        )

        list(manager.fetch_jobs(job_definition_name))

        mock_batch_client.list_namespaced_job.assert_called_once_with(
            namespace=namespace,
            label_selector=signer.label_selector(job_definition_name),
        )

    def test_read_job(self, mock_batch_client):
        namespace = "thisissparta"
        manager = JobManager(
            namespace=namespace, signer=Mock(), register=StaticJobDefinitionsRegister()
        )
        job_name = "xyzab"

        manager.read_job(job_name)

        mock_batch_client.read_namespaced_job_status.assert_called_once_with(
            name=job_name, namespace=namespace
        )

    def test_job_logs(self, mock_core_client):
        namespace = "treesbecomelogs"
        manager = JobManager(
            namespace=namespace, signer=Mock(), register=StaticJobDefinitionsRegister()
        )
        job_name = "ahoymatey"
        mock_core_client.list_namespaced_pod.return_value.items = [
            V1Pod(metadata=V1ObjectMeta(name="foo"))
        ]
        log_msg = "this is a log"
        mock_core_client.read_namespaced_pod_log.return_value = log_msg

        log = manager.job_logs(job_name)

        assert log_msg in log
        assert "foo" in log
        mock_core_client.list_namespaced_pod.assert_called_once_with(
            namespace=namespace, label_selector=f"job-name={job_name}"
        )
        mock_core_client.read_namespaced_pod_log.assert_called_once_with(
            name="foo",
            namespace=namespace,
            tail_lines=ANY,
            limit_bytes=ANY,
            pretty=True,
        )

    def test_job_logs_not_ready(self, mock_core_client):
        namespace = "notready"
        manager = JobManager(
            namespace=namespace, signer=Mock(), register=StaticJobDefinitionsRegister()
        )
        mock_core_client.list_namespaced_pod.return_value.items = [
            V1Pod(metadata=V1ObjectMeta(name="foo"))
        ]
        mock_core_client.read_namespaced_pod_log.side_effect = ApiException(
            http_resp=Mock(
                data={
                    "message": 'container "hello" in pod "hello-50ac1a4c086b2a4493081d5a55c6f5cf92ac5bb61c51fc4c-h5jsd" is waiting to start: ContainerCreating'
                }
            )
        )

        # No exception
        logs = manager.job_logs("whatever")

        assert logs == "Pod: foo\n"

    def test_job_logs_multiple(self, mock_core_client):
        namespace = "123"
        manager = JobManager(
            namespace=namespace, signer=Mock(), register=StaticJobDefinitionsRegister()
        )
        job_name = "takeyourhandandcomewithme"
        names = ["because", "you"]
        mock_core_client.list_namespaced_pod.return_value.items = [
            V1Pod(metadata=V1ObjectMeta(name=names[0])),
            V1Pod(metadata=V1ObjectMeta(name=names[1])),
        ]
        log_msgs = ["look", "so"]
        mock_core_client.read_namespaced_pod_log.side_effect = log_msgs

        log = manager.job_logs(job_name)

        assert all([name in log for name in names]), "Should print both pod names"
        assert all([log_msg in log for log_msg in log_msgs]), "Should print both logs"
        assert mock_core_client.read_namespaced_pod_log.call_count == 2

    def test_run_background_cleanup(self):
        manager = JobManager(namespace="foo", signer=Mock(), register=Mock())
        with patch.object(manager, "delete_old_jobs") as _:
            stop = manager.run_background_cleanup(0)

            stop()
