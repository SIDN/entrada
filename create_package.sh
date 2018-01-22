#!/usr/bin/env bash

VERSION=$(cat VERSION)
BASE_DIR="entrada-$VERSION"
echo "Create ENTRADA installation package for version $VERSION"

#make 100% sure the package dir does not exist
rm -rf $BASE_DIR

mkdir $BASE_DIR
cp pcap-to-parquet/target/pcap-to-parquet-$VERSION-jar-with-dependencies.jar $BASE_DIR
cp -R scripts $BASE_DIR
cp -R grafana-dashboard $BASE_DIR
cp VERSION $BASE_DIR
cp UPGRADE $BASE_DIR
cd $BASE_DIR
ln -s pcap-to-parquet-$VERSION-jar-with-dependencies.jar entrada-latest.jar
cd ..

tar -zcvf "$BASE_DIR.tar.gz" $BASE_DIR
rm -rf $BASE_DIR
