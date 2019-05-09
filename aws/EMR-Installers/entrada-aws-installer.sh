#!/usr/bin/env bash

# This bootstrap script is made to install a modified version of ENTRADA
# on the Amazon Web Services service, Elastic Map Reduce.
#
# ENTRADA, a big data platform for network data analytics
# Copyright (C) 2016 SIDN [https://www.sidn.nl]
#
# some other text idk
#

echo "[$(date)] : Starting Entrada AWS installation"

config=$1

sudo yum -y update

#install git and parallel
echo "[$(date)] : Downloading prerequisites"
sudo yum install -y parallel curl
sudo python3 -m pip install boto3

sudo chmod 770 -R /home/hadoop

#download the package, config and crontab
echo "[$(date)] : Downloading Entrada I"
cd /home/hadoop
sudo wget https://github.com/SIDN/entrada/releases/download/v0.0.1a/entrada-aws-0.1.2.tar.gz
tar -xzvf entrada-aws-0.1.2.tar.gz
ln -s entrada-0.1.2 entrada-latest
sudo aws s3 cp $config entrada-latest/scripts/run/config.sh

#load config
echo "[$(date)] : Loading config"
source entrada-latest/scripts/run/config.sh 2> /dev/null
#will throw error because of $NAMESERVERS, however this can be disregarded

sudo chown -R hadoop:hadoop ./
sudo chmod -R 770 ./

echo "[$(date)] : Creating tables - if metadata already exists these scripts will not change anything"
sh ./entrada-latest/scripts/install/create_s3External_tables.sh
sh ./entrada-latest/scripts/install/create_domain_stats_table_S3.sh
sh ./entrada-latest/scripts/install/get_s3data.sh
echo "[$(date)] : Table creation finished"

echo "[$(date)] : Entrada AWS installation complete"
exit 0
