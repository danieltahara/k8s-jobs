from flask import Flask

import kubernetes


def create_app(config_filename):
    app = Flask(__name__)
    app.config.from_pyfile(config_filename)

    kubernetes.config.load_kube_config(app.config["KUBE_CONFIG_PATH"])

    return app
