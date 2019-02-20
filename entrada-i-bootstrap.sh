#!/usr/bin/env bash

# This bootstrap script is made to install a modified version of ENTRADA
# on the Amazon Web Services service, Elastic Map Reduce v5.20.0.
#
# ENTRADA, a big data platform for network data analytics
# Copyright (C) 2016 SIDN [https://www.sidn.nl]
#
# some other text idk
#

echo "[$(date)] : Starting Entrada I bootstrap"

config=$1
crontab=$2

#install git and parallel
echo "[$(date)] : Downloading prerequisites"
sudo yum install -y git parallel

#download the package, config and crontab
echo "[$(date)] : Downloading Entrada I"
cd /home/hadoop
git clone https://EMR_CodeCommit-at-845534697080:R9GGhQEz2rcrMfYFmmX9TSTlNbbZnNKHzMBeiXb1OUs=@git-codecommit.eu-west-1.amazonaws.com/v1/repos/entrada-0.1.0-internetstiftelsen-0.1 entrada
aws s3 cp $config entrada/scripts/run/config.sh
aws s3 cp $crontab crontab.txt
ln -s entrada entrada-latest

#create directories for processing
mkdir ./pcap
mkdir ./pcap/processing
mkdir ./pcap/processed

mkdir ./tmp

sudo chown -R hadoop:hadoop ./
sudo chmod -R 700 ./

echo "[$(date)] : Creating tables"
# create tables using external s3 locations
sh ./entrada-latest/scripts/install/create_s3External_tables.sh && sh ./entrada-latest/scripts/install/create_domain_stats_table_S3.sh
# detect already existing partitions (and thereby data) on these tables
echo "[$(date)] : Gathering partitions"
hive -e "MSCK REPAIR TABLE $DNS_STAGING_TABLE; MSCK REPAIR TABLE $DNS_DWH_TABLE; MSCK REPAIR TABLE dns.domain_query_stats;"
# gather table statistics
echo "[$(date)] : Analyzing tables"
hive -e "
ANALYZE TABLE $DNS_STAGING_TABLE PARTITION(year, month, day, server) COMPUTE STATISTICS;
ANALYZE TABLE $DNS_STAGING_TABLE PARTITION(year, month, day, server) COMPUTE STATISTICS for columns;
ANALYZE TABLE $DNS_DWH_TABLE PARTITION(year, month, day, server) COMPUTE STATISTICS;
ANALYZE TABLE $DNS_DWH_TABLE PARTITION(year, month, day, server) COMPUTE STATISTICS for columns;
ANALYZE TABLE dns.domain_query_stats PARTITION(year, month, day) COMPUTE STATISTICS;
ANALYZE TABLE dns.domain_query_stats PARTITION(year, month, day) COMPUTE STATISTICS for columns;
"
echo "[$(date)] : Table creation finished"

#create log dir and set up logrotate
sudo mkdir -p /var/log/entrada
sudo chown hadoop:hadoop /var/log/entrada
sudo cat > /etc/logrotate.d/entrada << EOF
/var/log/entrada/*.log {
  size 10k
  daily
  maxage 10
  compress
  missingok
}
EOF

# create the cron table for hadoop
sudo bash -c "cat crontab.txt > /var/spool/cron/hadoop"

# for some reason below code did not work when giving a relative path (for the source), wont debug further for now since absolute path works
sudo sh -c 'source /home/hadoop/entrada-latest/scripts/run/config.sh && sh ./entrada-latest/scripts/run/run_update_geo_ip_db.sh'

echo "[$(date)] : Entrada I bootstrap complete"
exit 0
