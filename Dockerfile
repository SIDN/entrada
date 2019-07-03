FROM openjdk:8-jre
# Cannot use Alpine Linux because we need libc for Snappy.

LABEL maintainer="entrada@sidn.nl"

RUN mkdir -p /data/entrada/work
RUN mkdir -p /data/entrada/pcap
RUN mkdir -p /data/entrada/database
RUN mkdir -p /data/entrada/archive


# Make port 8080 available to the world outside this container
EXPOSE 8080

ARG JAR_FILE
ADD target/${JAR_FILE} /app/entrada.jar

ENV JAVA_OPTS=""

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar /app/entrada.jar"]