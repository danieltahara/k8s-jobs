from flask import Blueprint, jsonify

ops = Blueprint("ops", __name__, url_prefix="/ops")


@ops.route("/healthcheck", methods=["GET"])
def healthcheck():
    return jsonify({"healthy": True})
