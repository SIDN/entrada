#!/usr/bin/env bash

# ENTRADA, a big data platform for network data analytics
#
# Copyright (C) 2016 SIDN [https://www.sidn.nl]
#
# This file is part of ENTRADA.
#
# ENTRADA is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# ENTRADA is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with ENTRADA.  If not, see [<http://www.gnu.org/licenses/].

############################################################
##                                                        ##
##                                                        ##
##              ENTRADA configuration                     ##
##                                                        ##
##                                                        ##
############################################################

#home dir of entrada
export ENTRADA_HOME="/home/hadoop/entrada-latest"
#tmp dir used for keep state files
export TMP_DIR="$ENTRADA_HOME/tmp"

#decoder config file
export CONFIG_FILE="$ENTRADA_HOME/scripts/config/entrada-settings.properties"

#Impala deamon hostname for impala-shell to connect to
#export IMPALA_NODE="" edit: no need for this anymore

#hdfs locations for storing data
export HDFS_HOME="/user/hive/entrada"

#S3 locations for storing data
export S3_HOME="s3://dnspcaps"
export S3_BUCKET="dnspcaps"
export S3_PATH=""
export S3_ARCHIVE="s3://dnspcaps/archive"
##

#root directory for data input/output
export DATA_DIR="/data"
export OUTPUT_DIR="$ENTRADA_HOME/../processed"
#number of days to keep old pcap files
export PCAP_DAYS_TO_KEEP=10
#remove input pcap files from original location after they are processed
#if you are not using rsync to remove them then set this option to true.
export DELETE_INPUT_PCAP_FILES=false #edit: will want to set this to true in the future

#Log location
export ENTRADA_LOG_DIR="/var/log/entrada"

#name servers, seperate multiple NS with a semicolon ;
#or use the following line to automatically detect the name server sub directories.
#if using auto detect make sure the data for every (new) name server is uploaded
#in the correct temporal order.
export NAMESERVERS=`cd ${DATA_DIR}/processing; ls -dm */ | tr '\n' ' ' |  tr ',' ';' | sed 's/[\/ ]//g'`
#edit: changed it to use the processing folder instead of DATA_RSYNC_DIR

#java lib jar
export ENTRADA_JAR=$(cd $ENTRADA_HOME && ls pcap-to-parquet*)
#start and max heap size for entrada pcap convertor
export ENTRADA_HEAP_SIZE=4096m

#security if Kerberos is enabled, otherwise keep empty
# export KRB_USER=""
# export KEYTAB_FILE="my.keytab"

#error mail recipient
export ERROR_MAIL=""

#Max number of nameserver to process at the same time
#to prevent overloading the server
export PARALLEL_JOBS=3

#HDFS directories for table files
export HDFS_DNS_STAGING="$HDFS_HOME/staging"
# export HDFS_ICMP_STAGING="$HDFS_HOME/icmp-staging"
export HDFS_DNS_QUERIES="$HDFS_HOME/queries"
# export HDFS_ICMP_PACKETS="$HDFS_HOME/icmp"
export S3_DNS_STAGING="$S3_HOME/staging"
export S3_DNS_QUERIES="$S3_HOME/queries"

# table names
export DNS_STAGING_TABLE="dns.staging"
# export IMPALA_ICMP_STAGING_TABLE="icmp.staging"
export DNS_DWH_TABLE="dns.queries"
# export IMPALA_ICMP_DWH_TABLE="icmp.packets"
