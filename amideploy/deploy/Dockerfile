FROM apache/airflow:3.2.0-python3.12
USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential python3-dev \
    && apt-get clean

USER airflow

# requirements.txt references constraints-3.X.X.txt via --constraint, so it must
# be present in the build context alongside it.
COPY ./repo/requirements.txt ./repo/constraints-* ./
RUN pip install -r requirements.txt --no-cache-dir
