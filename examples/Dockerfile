FROM python:3.6.6
RUN apt-get update && apt-get -y dist-upgrade

RUN apt-get install busybox

RUN pip install --upgrade pip==19.2.3
RUN pip install poetry
RUN poetry config settings.virtualenvs.create false

ADD . /var/k8s-jobs
WORKDIR /var/k8s-jobs

# Include dev dependencies (flask). This is fine since it's a sample.
RUN poetry install

EXPOSE 5000

# You never want to actually run flask directly in production. At least put it behind
# UWSGI.
ENTRYPOINT ["/usr/local/bin/dumb-init", "--", "python"]

CMD ["examples/flask/app.py"]
