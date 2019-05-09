#!/usr/bin/env bash

#    This installer is made to install a modified version of ENTRADA on the
#    Amazon Web Services service, Elastic Map Reduce.
#
#    Copyright (C) 2019 Internetstiftelsen [https:/internetstiftelsen.se/en]
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.


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
