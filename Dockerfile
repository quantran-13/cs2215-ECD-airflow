FROM apache/airflow:2.7.2-python3.11

ARG CURRENT_USER=$USER

USER root
# Install Python dependencies to be able to process the wheels from the private PyPI server.
RUN apt-get -y update && apt-get -y upgrade
RUN apt-get install -y python3-distutils python3.11-dev build-essential
USER ${CURRENT_USER}


# Install any linux packages here (Optional)

USER root

RUN apt-get update -yqq \
    && apt-get install -y vim


# Add any python libraries  (Optional)
#https://airflow.apache.org/docs/apache-airflow/stable/extra-packages-ref.html?highlight=snowflake

USER airflow

RUN pip install \
    'python-dotenv' \
    'apache-airflow-providers-http==2.0.1' \
    'apache-airflow-providers-postgres==2.2.0'


USER airflow
