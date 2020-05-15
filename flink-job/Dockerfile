FROM maven:3.6-jdk-8-slim AS builder

# Get data producer code and compile it
COPY ./src /opt/src
COPY ./pom.xml /opt/pom.xml

RUN cd /opt; \
    mvn clean install

FROM flink:1.10.1-scala_2.12

COPY --from=builder /opt/target/flink-parquet-*.jar /opt/flink/usrlib/data-producer.jar

RUN mkdir /opt/flink/plugins/s3; \
    mv /opt/flink/opt/flink-s3-fs-hadoop-1.10.1.jar /opt/flink/plugins/s3; \
    echo "s3.access-key: demo-key" >> /opt/flink/conf/flink-conf.yaml; \
    echo "s3.secret-key: demo-secret" >> /opt/flink/conf/flink-conf.yaml; \
    echo "fs.s3a.access.key: demo-key" >> /opt/flink/conf/flink-conf.yaml; \
    echo "fs.s3a.secret.key: demo-secret" >> /opt/flink/conf/flink-conf.yaml; \
    echo "fs.s3a.endpoint: http://minio:9000" >> /opt/flink/conf/flink-conf.yaml; \
    echo "fs.s3a.path.style.access: true" >> /opt/flink/conf/flink-conf.yaml; \
    sed -i -e 's/taskmanager.memory.process.size: 1568m/taskmanager.memory.process.size: 1728m/g' /opt/flink/conf/flink-conf.yaml; \
    echo "taskmanager.memory.jvm-metaspace.size: 256m" >> /opt/flink/conf/flink-conf.yaml;
