# Use the official Airflow image
FROM apache/airflow:2.8.1

USER root

# Install Docker CLI inside the container
RUN apt-get update -qq && apt-get install -y docker.io 

COPY ./config/airflow.cfg /opt/airflow/airflow.cfg

USER airflow
