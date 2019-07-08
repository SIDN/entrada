FROM openjdk:8-jre
# Cannot use Alpine Linux because we need libc for Snappy.

LABEL maintainer="entrada@sidn.nl"

ENV APP_HOME /entrada

RUN mkdir $APP_HOME
RUN mkdir -p $APP_HOME/bin
RUN mkdir -p $APP_HOME/data/work
RUN mkdir -p $APP_HOME/data/input
RUN mkdir -p $APP_HOME/data/output
RUN mkdir -p $APP_HOME/data/archive
RUN mkdir -p $APP_HOME/data/log
RUN mkdir -p $APP_HOME/data/conf


# Make port 8080 available to the world outside this container
EXPOSE 8080

ARG JAR_FILE
ADD target/${JAR_FILE} $APP_HOME/bin/entrada.jar

ENV JAVA_OPTS=""

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar $APP_HOME/bin/entrada.jar"]