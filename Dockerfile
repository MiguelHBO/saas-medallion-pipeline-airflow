FROM apache/airflow:2.10.4-python3.11

# Keep pip's resolution consistent with what this Airflow release was tested
# against — installing our extra packages "bare" risks silently upgrading a
# dependency Airflow itself pins (SQLAlchemy being the classic offender).
ARG AIRFLOW_VERSION=2.10.4
ARG PYTHON_VERSION=3.11
ARG CONSTRAINTS_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

COPY requirements.txt /requirements.txt

USER airflow
RUN pip install --no-cache-dir --constraint "${CONSTRAINTS_URL}" -r /requirements.txt
