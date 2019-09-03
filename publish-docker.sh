#!/usr/bin/env bash

cd ../dns-lib
mvn clean install

cd ../pcap-lib
mvn clean install

cd ../entrada

mvn clean package

mvn dockerfile:build
mvn verify
mvn dockerfile:push
