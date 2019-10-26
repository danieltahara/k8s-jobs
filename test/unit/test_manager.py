from datetime import datetime
import time
from unittest.mock import ANY, Mock, patch

from kubernetes.client import (
    V1Container,
    V1DeleteOptions,
    V1Job,
    V1JobCondition,
    V1JobList,
    V1JobStatus,
    V1ListMeta,
    V1ObjectMeta,
    V1Pod,
    V1PodSpec,
)
from kubernetes.client.rest import ApiException
import pytest

from k8s_jobs.manager import (
    JobDeleter,
    JobManager,
    JobSigner,
    NotFoundException,
    StaticJobDefinitionsRegister,
)


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

    def test_label_selector_extra(self):
        signature = "woahhhh"
        signer = JobSigner(signature)

        assert signer.label_selector() == f"{JobSigner.LABEL_KEY}={signature}"

        job_definition_name = "jdphd"
        assert (
            signer.label_selector(extra="label", another=2)
            == f"{signer.label_selector()},extra=label,another=2"
        )

        assert (
            signer.label_selector(job_definition_name, extra="label", another=2)
            == f"{signer.label_selector()},{JobSigner.JOB_DEFINITION_NAME_KEY}={job_definition_name},extra=label,another=2"
        )

        # Various permutations are safe
        assert (
            signer.label_selector(
                extra="label", another=2, job_definition_name=job_definition_name
            )
            == f"{signer.label_selector()},{JobSigner.JOB_DEFINITION_NAME_KEY}={job_definition_name},extra=label,another=2"
        )
        assert (
            signer.label_selector(
                job_definition_name=job_definition_name, extra="label", another=2
            )
            == f"{signer.label_selector()},{JobSigner.JOB_DEFINITION_NAME_KEY}={job_definition_name},extra=label,another=2"
        )


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

    def test_create_job_callback(self, mock_batch_client):
        manager = JobManager(
            namespace="hellomoto",
            signer=Mock(),
            register=StaticJobDefinitionsRegister({"g1": Mock()}),
        )
        create_callback = Mock()

        manager.create_job("g1", pre_create=create_callback)

        create_callback.assert_called_once()

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

    def test_job_is_finished(self):
        manager = JobManager(namespace="xyz", signer=Mock(), register=Mock())

        job = V1Job(status=V1JobStatus(conditions=[]))
        assert not manager.job_is_finished(job)

        job = V1Job(status=V1JobStatus(conditions=[], completion_time=datetime.now()))
        assert not manager.job_is_finished(job), "Completion time field is unchecked"

        job = V1Job(
            status=V1JobStatus(
                conditions=[V1JobCondition(status="True", type="Complete")]
            )
        )
        assert manager.job_is_finished(job), "A complete job should be finished"

        job = V1Job(
            status=V1JobStatus(
                conditions=[V1JobCondition(status="False", type="Complete")]
            )
        )
        assert not manager.job_is_finished(
            job
        ), "False job status conditions should be ignored"

        job = V1Job(
            status=V1JobStatus(
                conditions=[V1JobCondition(status="True", type="Failed")]
            )
        )
        assert manager.job_is_finished(job), "A failed job is finished"

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

    def test_fetch_jobs_filters(self, mock_batch_client):
        mock_batch_client.list_namespaced_job.return_value = V1JobList(
            items=[V1Job(metadata=V1ObjectMeta(name="1"))], metadata=V1ListMeta()
        )
        namespace = "hellomoto"
        signer = JobSigner("foo")
        manager = JobManager(
            namespace=namespace, signer=signer, register=StaticJobDefinitionsRegister()
        )

        assert len(list(manager.fetch_jobs(extra="filter"))) == 1
        mock_batch_client.list_namespaced_job.assert_called_once_with(
            namespace=namespace, label_selector=signer.label_selector(extra="filter")
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

    def test_job_logs_multiple_containers(self, mock_core_client):
        namespace = "treesbecomelogs"
        manager = JobManager(
            namespace=namespace, signer=Mock(), register=StaticJobDefinitionsRegister()
        )
        job_name = "ahoymatey"
        pod_name = "p"
        container_name_1, container_name_2 = "c1", "c2"
        mock_core_client.list_namespaced_pod.return_value.items = [
            V1Pod(
                metadata=V1ObjectMeta(name=pod_name),
                spec=V1PodSpec(
                    containers=[
                        V1Container(name=container_name_1),
                        V1Container(name=container_name_2),
                    ]
                ),
            )
        ]
        log_msg = "this is a log"
        mock_core_client.read_namespaced_pod_log.return_value = log_msg

        logs = manager.job_logs(job_name)

        assert logs == {
            pod_name: {container_name_1: [log_msg], container_name_2: [log_msg]}
        }

    def test_job_logs_multiple_pods(self, mock_core_client):
        namespace = "treesbecomelogs"
        manager = JobManager(
            namespace=namespace, signer=Mock(), register=StaticJobDefinitionsRegister()
        )
        job_name = "ahoymatey"
        pod_name_1, pod_name_2 = "p1", "p2"
        container_name = "c1"
        mock_core_client.list_namespaced_pod.return_value.items = [
            V1Pod(
                metadata=V1ObjectMeta(name=pod_name_1),
                spec=V1PodSpec(containers=[V1Container(name=container_name)]),
            ),
            V1Pod(
                metadata=V1ObjectMeta(name=pod_name_2),
                spec=V1PodSpec(containers=[V1Container(name=container_name)]),
            ),
        ]
        log_msg = "this is a log"
        mock_core_client.read_namespaced_pod_log.return_value = log_msg

        logs = manager.job_logs(job_name)

        assert logs == {
            pod_name_1: {container_name: [log_msg]},
            pod_name_2: {container_name: [log_msg]},
        }

    def test_job_logs_not_ready(self, mock_core_client):
        namespace = "notready"
        manager = JobManager(
            namespace=namespace, signer=Mock(), register=StaticJobDefinitionsRegister()
        )
        pod_name = "p"
        container_name = "c"
        mock_core_client.list_namespaced_pod.return_value.items = [
            V1Pod(
                metadata=V1ObjectMeta(name=pod_name),
                spec=V1PodSpec(containers=[V1Container(name=container_name)]),
            )
        ]
        mock_core_client.read_namespaced_pod_log.side_effect = ApiException(
            http_resp=Mock(
                data={
                    "message": f'container "{container_name}" in pod "{pod_name}" is waiting to start: ContainerCreating'
                }
            )
        )

        # No exception
        logs = manager.job_logs("whatever")

        assert logs == {pod_name: {container_name: ["ContainerCreating"]}}


class TestJobDeleter:
    def test_mark_deletion_time(self, mock_batch_client):
        name = "deletionjob"
        namespace = "abcxyz"
        manager = Mock(namespace=namespace)
        deleter = JobDeleter(manager)

        job = V1Job(metadata=V1ObjectMeta(name=name, annotations={}))
        deleter.mark_deletion_time(job, 3600)
        mock_batch_client.patch_namespaced_job.assert_called_once_with(
            name=name, namespace=namespace, body=ANY
        )
        deletion_time_1 = mock_batch_client.patch_namespaced_job.call_args[1][
            "body"
        ].metadata.annotations[deleter.JOB_DELETION_TIME_ANNOTATION]
        mock_batch_client.reset_mock()

        job = V1Job(metadata=V1ObjectMeta(name=name, annotations={}))
        deleter.mark_deletion_time(job, 0)
        deletion_time_2 = mock_batch_client.patch_namespaced_job.call_args[1][
            "body"
        ].metadata.annotations[deleter.JOB_DELETION_TIME_ANNOTATION]

        assert int(deletion_time_1) > int(deletion_time_2)

    def test_mark_deletion_time_existing_annotation(self, mock_batch_client):
        name = "deletionjobalreadyannotated"
        namespace = "xyzabc"
        manager = Mock(namespace=namespace)
        deleter = JobDeleter(manager)
        job = V1Job(
            metadata=V1ObjectMeta(
                name=name, annotations={JobDeleter.JOB_DELETION_TIME_ANNOTATION: 0}
            )
        )

        deleter.mark_deletion_time(job, 0)

        mock_batch_client.patch_namespaced_job.assert_not_called()

    def test_mark_jobs_for_deletion(self, mock_batch_client):
        manager = Mock()
        manager.fetch_jobs.return_value = [Mock(), Mock()]
        manager.job_is_finished.side_effect = [True, False]
        deleter = JobDeleter(manager)

        with patch.object(deleter, "mark_deletion_time") as mock_mark_deletion_time:
            deleter.mark_jobs_for_deletion(0)

            mock_mark_deletion_time.assert_called_once()

        assert manager.job_is_finished.call_count == 2

    def test_is_candidate_for_deletion(self):
        deleter = JobDeleter(Mock())

        # No annotation
        assert not deleter.is_candidate_for_deletion(V1Job(metadata=V1ObjectMeta()))
        assert not deleter.is_candidate_for_deletion(
            V1Job(metadata=V1ObjectMeta(annotations={}))
        )
        # Wayyyy in the past
        assert deleter.is_candidate_for_deletion(
            V1Job(
                metadata=V1ObjectMeta(
                    annotations={JobDeleter.JOB_DELETION_TIME_ANNOTATION: 0}
                )
            )
        )
        # Far in the future
        assert not deleter.is_candidate_for_deletion(
            V1Job(
                metadata=V1ObjectMeta(
                    annotations={
                        JobDeleter.JOB_DELETION_TIME_ANNOTATION: int(
                            time.time() + 10000
                        )
                    }
                )
            )
        )

    def test_cleanup_jobs_error(self):
        manager = Mock()
        manager.fetch_jobs.return_value = [Mock(), Mock()]
        manager.delete_job.side_effect = [ApiException, None]
        deleter = JobDeleter(manager)

        with patch.object(deleter, "is_candidate_for_deletion", return_value=True):
            # Should not raise
            deleter.cleanup_jobs()

        assert manager.delete_job.call_count == 2

    def test_cleanup_jobs_callback(self):
        manager = Mock()
        manager.fetch_jobs.return_value = [Mock(), Mock()]
        deleter = JobDeleter(manager)
        mock_callback = Mock()

        with patch.object(deleter, "is_candidate_for_deletion", return_value=True):
            deleter.cleanup_jobs(delete_callback=mock_callback)

        assert mock_callback.call_count == 2

    def test_run_background_cleanup(self):
        deleter = JobDeleter(Mock())
        with patch.object(deleter, "mark_and_delete_old_jobs") as _:
            stop = deleter.run_background_cleanup(0)

            stop()
