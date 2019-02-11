#!/usr/bin/env bash

# This bootstrap script is made to install a modified version of ENTRADA
# on the Amazon Web Services service, Elastic Map Reduce v5.20.0.
#
# ENTRADA, a big data platform for network data analytics
# Copyright (C) 2016 SIDN [https://www.sidn.nl]
#
# some other text idk
#

yum install -y git

#download the package
cd /home/hadoop
git clone https://markus.videfors-at-845534697080:ahs+HEzJwAYKy8zhlPvHrGSPPpNR44+Kd7+q+rUJbE4=@git-codecommit.eu-west-1.amazonaws.com/v1/repos/Internetstiftelsen_ENTRADA
tar -xzvf packagePH
ln -s packagePH entrada-latest

source packagePH/bootstrapConfig.sh

#create directories for processing
mkdir ./pcap
mkdir ./pcap/processing
mkdir ./pcap/processed

for nameserver in $nservers
do
  mkdir ./pcap/processing/$nameserver
done

#create directory on hdfs for entrada
HADOOP_USER_NAME=hdfs
hdfs dfs -mkdir /user/hive/entrada
hdfs dfs -chown hadoop:hive /user/hive/entrada

#create the tables on hdfs
sh ./entrada-latest/scripts/install/create_impala_tables.sh
###
# need to add a function to import data from s3 into the tables created above, should be possible to just create the folder by partition and move the file
###

#create log dir and set up logrotate
mkdir -p /var/log/entrada
chown hadoop:hadoop /var/log/entrada
cat > /etc/logrotate.d/entrada << EOF
/var/log/entrada/*.log {
  size 10k
  daily
  maxage 10
  compress
  missingok
}
EOF


cd /home/hadoop/entrada-latest/scripts/run/
source config.sh && sh run_update_geo_ip_db.sh



































#
