FROM apache/airflow:2.9.3-python3.11

USER root
# Install Java for Spark
RUN apt-get update \
  && apt-get install -y default-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==4.8.2 pyspark==3.5.0
