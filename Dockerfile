FROM apache/airflow:2.8.1

# Switch to root to install system packages
USER root

# Install OpenJDK 17 — available in Debian Bookworm, compatible with PySpark 3.3+
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Tell Spark and PySpark where Java lives
# ARM64 path for Apple Silicon Macs
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Switch back to airflow user — never run Airflow as root
USER airflow

# Install PySpark — 3.3.0 is stable and widely deployed
# delta-spark 2.2.0 matches PySpark 3.3.0 compatibility matrix
RUN pip install --no-cache-dir \
    pyspark==3.3.0 \
    delta-spark==2.2.0
