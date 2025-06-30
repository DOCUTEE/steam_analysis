FROM python:3.11-slim

RUN apt-get update && apt-get install -y git  # <--- Cần git
RUN pip install dbt-core dbt-spark dbt-spark[PyHive]

RUN mkdir -p /dbt
WORKDIR /dbt
