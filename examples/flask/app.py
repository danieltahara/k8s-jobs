from flask import Flask, jsonify, request
import kubernetes

from k8s_jobs.config import JobManagerFactory

app = Flask(__name__)


@app.route("/jobs", methods=["POST"])
def create(job_definition_name: str):
    body = request.get_json()

    job_definition_name = body.pop("job_definition_name")

    job_name = app.manager.create_job(job_definition_name, template_args=body)

    return jsonify({"job_name": job_name})


@app.route("/jobs", methods=["GET"])
def list():
    job_definition_name = request.args.get("job_definition_name", None)

    jobs = app.manager.fetch_jobs(job_definition_name)

    return jsonify({"jobs": {job.metadata.name: job.metadata.status for job in jobs}})


@app.route("/jobs/<job_name>/status", methods=["GET"])
def status(job_name: str):
    status = app.manager.job_status()

    return jsonify(status)


@app.route("/jobs/<job_name>/logs", methods=["GET"])
def logs(job_name: str):
    # NOT JSON!
    return app.manager.job_logs(job_name=job_name)


@app.route("/healthcheck", methods=["GET"])
def healthcheck():
    return jsonify({"healthy": True})


if __name__ == "__main__":
    kubernetes.config.load_kube_config()

    config = JobManagerFactory.from_env()

    manager = config.generate()

    retention_period_sec = int(app.config.get("JOB_RETENTION_PERIOD_SEC", "3600"))
    _ = manager.run_background_cleanup(
        retention_period_sec=retention_period_sec, delete_callback=print
    )

    app.manager = manager

    app.run("0.0.0.0")
