#!/usr/bin/env bash

VERSION=$(cat VERSION)
BASE_DIR="entrada-$VERSION"
echo "Create ENTRADA installation package for version $VERSION"

mkdir $BASE_DIR
cp pcap-to-parquet/target/pcap-to-parquet-0.0.1-jar-with-dependencies.jar $BASE_DIR
cp -R scripts $BASE_DIR
cp -R grafana-dashboard $BASE_DIR
cp VERSION $BASE_DIR

tar -zcvf "$BASE_DIR.tar.gz" $BASE_DIR
rm -rf $BASE_DIR