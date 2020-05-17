FROM maven:3.6-jdk-8-slim AS builder

# Get data producer code and compile it
COPY ./src /opt/data-producer/src
COPY ./pom.xml /opt/data-producer/pom.xml

RUN cd /opt/data-producer; \
    mvn clean install

FROM openjdk:8-jre

COPY --from=builder /opt/data-producer/target/data-generator-*.jar /opt/data-generator.jar

RUN cd /opt

COPY docker-entrypoint.sh /

ENTRYPOINT ["/docker-entrypoint.sh"]
