FROM flink:1.20.3-scala_2.12-java17

# Keep connectors on the same Flink release line. JDBC also needs its database driver.
ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar /opt/flink/lib/
ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.3.0-1.20/flink-connector-jdbc-3.3.0-1.20.jar /opt/flink/lib/
ADD https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar /opt/flink/lib/
USER root
RUN chmod 644 /opt/flink/lib/*.jar
USER flink
COPY flink /opt/flink/aml
