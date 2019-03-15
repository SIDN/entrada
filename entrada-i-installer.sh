#!/usr/bin/env bash

# This bootstrap script is made to install a modified version of ENTRADA
# on the Amazon Web Services service, Elastic Map Reduce v5.20.0.
#
# ENTRADA, a big data platform for network data analytics
# Copyright (C) 2016 SIDN [https://www.sidn.nl]
#
# some other text idk
#

echo "[$(date)] : Starting Entrada I installation"

config=$1

sudo yum -y update

#install git and parallel
echo "[$(date)] : Downloading prerequisites"
sudo yum install -y git parallel curl
sudo python3 -m pip install boto3

sudo chmod 777 -R /home/hadoop

#download the package, config and crontab
echo "[$(date)] : Downloading Entrada I"
cd /home/hadoop
sudo git clone https://EMR_CodeCommit-at-845534697080:R9GGhQEz2rcrMfYFmmX9TSTlNbbZnNKHzMBeiXb1OUs=@git-codecommit.eu-west-1.amazonaws.com/v1/repos/entrada-i entrada-latest
sudo aws s3 cp $config entrada-latest/scripts/run/config.sh

#load config
echo "[$(date)] : Loading config"
source entrada-latest/scripts/run/config.sh 2> /dev/null

sudo chown -R hadoop:hadoop ./
sudo chmod -R 700 ./

echo "[$(date)] : Creating tables"
sh ./entrada-latest/scripts/install/create_s3External_tables.sh
sh ./entrada-latest/scripts/install/create_domain_stats_table_S3.sh
sh ./entrada-latest/scripts/install/get_s3data.sh
echo "[$(date)] : Table creation finished"

echo "[$(date)] : Entrada I installation complete"
exit 0
