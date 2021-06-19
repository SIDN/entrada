#!/usr/bin/env bash

export JAVA_HOME=`/usr/libexec/java_home -v 11.0.3`

cd ../dns-lib
mvn clean install

cd ../pcap-lib
mvn clean install

cd ../entrada

mvn clean package

mvn dockerfile:build
mvn verify
mvn dockerfile:push
