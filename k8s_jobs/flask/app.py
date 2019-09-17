from flask import Flask

import kubernetes

from k8s_jobs.k8s.job import JobSigner, JobManager


def create_app(config):
    kubernetes.config.load_kube_config()

    app = Flask(__name__)
    app.config.from_object(config)

    namespace = app.config["JOB_NAMESPACE"]
    signer = JobSigner(app.config["JOB_SIGNATURE"])
    retention_period_sec = int(app.config.get("JOB_RETENTION_PERIOD_SEC", "3600"))
    manager = JobManager(namespace, signer, {})
    _ = manager.run_background_cleanup(retention_period_sec=retention_period_sec)
    app.manager = manager

    from k8s_jobs.flask.jobs import jobs

    app.register_blueprint(jobs)
    from k8s_jobs.flask.ops import ops

    app.register_blueprint(ops)

    return app
