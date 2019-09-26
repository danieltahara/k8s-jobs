K8s Jobs
=========

.. image:: https://travis-ci.com/danieltahara/k8s-jobs.svg?token=cZTmQ2jMoLFe6Ve33X6M&branch=master

K8s Jobs is a library for implementing an asynchronous job server on Kubernetes. It is
intended to provide a simple framework for executing single-shot asynchronous jobs or
commands (unlike something like Celery that can have arbitrary fanout and nesting), as
well as a server implementation that can stand in as a replacement for AWS Batch and
trigger jobs on-command.

What's Inside
-------------

Kubernetes Job Management
+++++++++++++++++++++++++

This project provides an abstraction around kubernetes APIs to allow you to dynamically
spawn (templated) jobs and clean up after them when they have run.

The two abstractions of interest are the ``JobManager`` and ``JobManagerFactory``. The
latter provides a factory for the former and helps convert (kubernetes) configuration
into a working application.

The ``JobManager`` is responsible for creating (templated) jobs given a job definition
name and template arguments. It is recommended that the jobs target a dedicated node
instance group so as not to contend with live application resources. It is further
recommended that you configure the `Cluster Autoscaler
<https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler>`_ on this
instance group to ensure you do not run out of capacity (even better would be something
like the `Escalator <https://github.com/atlassian/escalator>`_, a batch job-oriented
autoscaler). This will most closely mirror the behavior of a service like AWS Batch,
which automatically adjusts the number of nodes based on workload.

The ``JobManager`` is also responsible for cleaning up terminated (completed or failed)
jobs after some retention period. This is provided for version compatibility across K8s,
and to avoid using a feature that is still in Alpha. For more details, see the `TTL
Controller
<https://kubernetes.io/docs/concepts/workloads/controllers/ttlafterfinished/>`_.

Example Flask Server
++++++++++++++++++++

The server is a proof-of-concept impleemtnation intended as a replacement for and
extension to AWS Batch. It is a flask application housed completely under
``examples/flask``. You do not need to use the server in order to take advantage of the
primitives on which it relies.

The server listens on a route for job creation requests, much in the same way AWS batch
might be implemented under the hood.

Kubernetes Resources
++++++++++++++++++++

The Kubernetes resources under ``examples/k8s/`` provide the configuration needed for
deploying a server to Kubernetes. Specifically, it demonstrates how to configure jobs to
be run by the manager.  It relies on ConfigMap volume mounts in order to load the
templates into a consistent location. See the ``JobManagerFactory`` for the specific
required structure.

There is a corresponding dockerfile at ``examples/Dockerfile`` that can be used with the
templates. You can build it as follows:

.. code::

   sudo docker build -t flask-app examples

QuickStart
----------

To install dependencies:

.. code:: bash

  poetry install

To run the sample server locally (make sure you have ``~/.kube/config`` configured):

.. code:: bash

  JOB_SIGNATURE=foo JOB_NAMESPACE=default JOB_DEFINITIONS_CONFIG_PATH=path/to/conf python examples/flask/app.py
