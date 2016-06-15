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
export ENTRADA_HOME="/home/entrada/entrada-latest"
#tmp dir used for keep state files
export TMP_DIR="$ENTRADA_HOME/tmp/"

#decoder config file
export CONFIG_FILE="$ENTRADA_HOME/scripts/config/entrada-settings.properties"

#Impala deamon hostname for impala-shell to connect to
export IMPALA_NODE=""

#hdfs locations for storing data
export HDFS_HOME="/user/hive/entrada/"

#input directories, subdirs must have same name as name server
export DATA_RSYNC_DIR="/home/entrada/captures"
#root directory for data input/output
export DATA_DIR="/home/entrada/pcap"
#number of days to keep old pcap files
export PCAP_DAYS_TO_KEEP=10

#Log location
export ENTRADA_LOG_DIR="/var/log/entrada"

#name servers, seperate multiple NS with a colon ;
export NAMESERVERS=`cd ${DATA_RSYNC_DIR}; ls -dm */ | tr '\n' ' ' |  tr ',' ';' | sed 's/[\/ ]//g'`

#java
export ENTRADA_JAR="pcap-to-parquet-0.0.3-jar-with-dependencies.jar"

#security if Kerberos is enabled, otherwise keep empty
export KRB_USER=""
export KEYTAB_FILE="my.keytab"

#error mail recipient
export ERROR_MAIL=""
