FROM apache/spark:4.1.4-scala2.13-java17-python3-ubuntu

# Iceberg (tables + S3 client) and the Kafka source, matched to Spark 4.1 / Scala 2.13.
ARG MAVEN=https://repo1.maven.org/maven2
USER root
ADD ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.12.0/iceberg-spark-runtime-4.1_2.13-1.12.0.jar \
    ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar \
    ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.4/spark-sql-kafka-0-10_2.13-4.1.4.jar \
    ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.4/spark-token-provider-kafka-0-10_2.13-4.1.4.jar \
    ${MAVEN}/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar \
    ${MAVEN}/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar \
    /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/*.jar && mkdir -p /checkpoints && chown spark /checkpoints
COPY infra/spark/spark-defaults.conf infra/spark/log4j2.properties /opt/spark/conf/
COPY python/pipeline.py /opt/app/pipeline.py

USER spark
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[2]", "/opt/app/pipeline.py"]
CMD ["run"]
