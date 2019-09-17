from flask import Blueprint, current_app as app, jsonify, request

jobs = Blueprint("jobs", __name__, url_prefix="/jobs")


@jobs.route("/jobs", methods=["GET"])
def list():
    jobs = app.manager.fetch_jobs()
    return jsonify({"jobs": {job.metadata.name: job.metadata.status for job in jobs}})


@jobs.route("/jobs/<job_definition_name>", methods=["POST"])
def create(job_definition_name: str):
    body = request.get_json()

    job_name = app.manager.create_job(job_definition_name, template_args=body)

    return jsonify({"job_name": job_name})
