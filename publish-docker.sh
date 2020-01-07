#!/usr/bin/env bash

export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_192.jdk/Contents/Home/

cd ../dns-lib
mvn clean install

cd ../pcap-lib
mvn clean install

cd ../entrada

mvn clean package

mvn dockerfile:build
mvn verify
mvn dockerfile:push
