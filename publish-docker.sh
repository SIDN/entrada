#!/usr/bin/env bash

export TOOL_VERSION=$1
echo "Build ENTRADA version $TOOL_VERSION"

export JAVA_HOME=`/usr/libexec/java_home -v 21`
export MAVEN_OPTS="--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.desktop/java.awt.font=ALL-UNNAMED"

mvn clean package

docker build --tag=sidnlabs/entrada:$TOOL_VERSION .
docker push sidnlabs/entrada:$TOOL_VERSION