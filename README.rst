K8s Async
---

K8s Async is a library and server implementation for asynchronous jobs on Kubernetes. It is intended
to provide a simple framework for executing single-shot asynchronous jobs or commands (unlike
something like Celery that can have arbitrary fanout and nesting), as well as a server
implementation that can stand in as a replacement for AWS Batch and trigger jobs on-command.

What's Inside
===

Worker
+++
The worker consists of a configurable queue consumer that reads messages off of a queue and either
processes a single message before exiting, or runs as a daemon or until some stop condition is met.

The queue libraries can be used standalone, but they are primarily intended for use in the context
of a 'worker' as the basis of a one-off Kubernetes job or a daemon as part of a Kubernetes
deployment.

Server
+++

The server listens on a route for job creation requests, much in the same way AWS batch might be
implemented under the hood. Given a request, it will do some combination of enqueueing the message
onto a queue dedicated for that job type and spawning a job pod to provide capacity to serve that
job. Both are optional. For example, the job pod might not need any external context to execute, or
it could be configured to check some external state, with the server simply triggering it. This
would obviate the need for writing the job message to a queue. Alternatively, a backend service
could be configured with a set of worker daemons, in which case the per-job capacity is unnecessary
and a job pod need not be created.

QuickStart
===

TODO:
