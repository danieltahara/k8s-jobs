import yaml

from flask import Flask
import kubernetes

from k8s_jobs.flask.jobs import jobs
from k8s_jobs.flask.ops import ops
from k8s_jobs.k8s.config import JobDefinitionsConfig
from k8s_jobs.k8s.job import JobSigner, JobManager


def create_app(config):
    kubernetes.config.load_kube_config()

    app = Flask(__name__)
    app.config.from_object(config)

    namespace = app.config["JOB_NAMESPACE"]
    signer = JobSigner(app.config["JOB_SIGNATURE"])

    with open(app.config["JOB_DEFINITIONS_PATH"]) as f:
        job_definitions = yaml.safe_load(f)

    # TODO: Set default on config object
    config_root = app.config.get("JOB_DEFINITIONS_CONFIG_ROOT", "/etc/config")

    config = JobDefinitionsConfig(job_definitions, config_root)

    manager = JobManager(namespace, signer, config.make_generators())

    retention_period_sec = int(app.config.get("JOB_RETENTION_PERIOD_SEC", "3600"))
    _ = manager.run_background_cleanup(retention_period_sec=retention_period_sec)

    app.manager = manager

    app.register_blueprint(jobs)
    app.register_blueprint(ops)

    return app
