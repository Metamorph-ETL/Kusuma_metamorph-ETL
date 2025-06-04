FROM quay.io/astronomer/astro-runtime:13.0.0

USER root

# Install required dependencies
RUN apt update && \
    apt-get install -y openjdk-17-jdk ant wget libpq-dev && \
    apt-get clean

# Install GCS connector for Hadoop
RUN mkdir -p /usr/local/airflow/jars/ && \
    wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar -P /usr/local/airflow/jars/

# Set Java environment variables
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Set path to credentials for GCS access
ENV GCS_CREDENTIALS_PATH="/usr/local/airflow/jars/meta-morph-d-eng-pro-view-key.json"

USER astro
