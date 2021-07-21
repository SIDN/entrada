FROM openjdk:11
# Cannot use Alpine Linux because we need libc for Snappy.

LABEL maintainer="entrada@sidn.nl"

ENV APP_HOME /entrada

RUN mkdir $APP_HOME
RUN mkdir -p $APP_HOME/data/
RUN mkdir -p $APP_HOME/bin

# Make port 8080 available to the world outside this container
EXPOSE 8080
EXPOSE 9999

ARG JAR_FILE
ADD target/${JAR_FILE} $APP_HOME/bin/entrada.jar

ENV JAVA_OPTS=""

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -XX:AutoBoxCacheMax=10000 -XX:+PrintFlagsFinal -XX:+PrintStringTableStatistics -jar $APP_HOME/bin/entrada.jar"]