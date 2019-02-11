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
git clone https://EMR_CodeCommit-at-845534697080:R9GGhQEz2rcrMfYFmmX9TSTlNbbZnNKHzMBeiXb1OUs=@git-codecommit.eu-west-1.amazonaws.com/v1/repos/entrada-0.1.0-internetstiftelsen-0.1 entrada-I-0.1

chown -R hadoop:hadoop entrada-I-0.1
chmod -R 700 entrada-I-0.1

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
