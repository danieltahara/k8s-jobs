from flask import Flask, jsonify, request
import kubernetes

from k8s_jobs.manager import JobManager, JobSigner

app = Flask(__name__)


@app.route("/jobs", methods=["GET"])
def list():
    jobs = app.manager.fetch_jobs()
    return jsonify({"jobs": {job.metadata.name: job.metadata.status for job in jobs}})


@app.route("/jobs/<job_definition_name>", methods=["POST"])
def create(job_definition_name: str):
    body = request.get_json()

    job_name = app.manager.create_job(job_definition_name, template_args=body)

    return jsonify({"job_name": job_name})


@app.route("/healthcheck", methods=["GET"])
def healthcheck():
    return jsonify({"healthy": True})


if __name__ == "__main__":
    kubernetes.config.load_kube_config()

    app = Flask(__name__)

    signer = JobSigner("signer")
    manager = JobManager("default", signer, {})
    retention_period_sec = int(app.config.get("JOB_RETENTION_PERIOD_SEC", "3600"))
    _ = manager.run_background_cleanup(retention_period_sec=retention_period_sec)

    app.manager = manager
