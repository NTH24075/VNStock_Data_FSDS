# Flink 1.18 with Python 3 + PyFlink + Kafka connector JAR
FROM flink:1.18-scala_2.12

USER root

# Install Python 3 for PyFlink
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-venv curl \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/bin/python3 /usr/bin/python

# Install PyFlink Python packages (must match Flink 1.18)
RUN python -m pip install --no-cache-dir \
    apache-flink==1.18.0 \
    apache-flink-libraries==1.18.0 \
    kafka-python

# Download Kafka SQL connector JAR (not a pip package — must be downloaded from Maven)
# Matches Flink 1.18 + Kafka connector version
ENV FLINK_HOME=/opt/flink
RUN curl -fsSL -o ${FLINK_HOME}/lib/flink-sql-connector-kafka-3.0.2-1.18.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar

ENV PATH="${FLINK_HOME}/bin:${PATH}"

USER flink
