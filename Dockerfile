FROM --platform=linux/amd64 openjdk:21
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
ADD target/entrada-*.jar $APP_HOME/bin/entrada.jar

ENV JAVA_OPTS=""
ENV JAVA_PRE_OPTS -Djava.security.egd=file:/dev/./urandom \
    -XX:AutoBoxCacheMax=10000 \
    -XX:+PrintFlagsFinal \
    -XX:+PrintStringTableStatistics \
    --add-opens java.base/java.lang=ALL-UNNAMED \
    --add-opens java.base/java.security=ALL-UNNAMED \
    --add-opens java.base/java.security.cert=ALL-UNNAMED \
    --add-opens java.base/java.security.spec=ALL-UNNAMED \
	--add-opens java.base/java.util=ALL-UNNAMED \
	--add-opens java.base/java.net=ALL-UNNAMED \
	--add-opens java.base/sun.security.x509=ALL-UNNAMED \
	--add-opens java.base/sun.security.util=ALL-UNNAMED \
	--add-opens java.base/javax.security.auth.x500=ALL-UNNAMED \
	--add-opens java.base/sun.security.rsa=ALL-UNNAMED \
	--add-opens jdk.crypto.ec/sun.security.ec=ALL-UNNAMED
	

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS $JAVA_PRE_OPTS -jar $APP_HOME/bin/entrada.jar"]