# Spark image shared by the driver (spark-iceberg), the standalone master and
# the workers, so Spark/Scala/Python/jar versions are identical everywhere.
# Base already has Spark 3.5.5, Iceberg 1.8.1 and the AWS bundle; we add the
# Kafka source for Structured Streaming.
FROM oci.mydigipay.info/tabulario/spark-iceberg:3.5.5_1.8.1

ARG MVN=https://repo1.maven.org/maven2
ADD ${MVN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar /opt/spark/jars/
ADD ${MVN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.5/spark-token-provider-kafka-0-10_2.12-3.5.5.jar /opt/spark/jars/
ADD ${MVN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD ${MVN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.5.jar \
              /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.5.jar \
              /opt/spark/jars/kafka-clients-3.4.1.jar \
              /opt/spark/jars/commons-pool2-2.11.1.jar
