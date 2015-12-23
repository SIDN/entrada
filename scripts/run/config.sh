#!/usr/bin/env bash

############################################################
##                                                        ##
##                                                        ##
##              ENTRADA configuration                     ##
##                                                        ##
##                                                        ##
############################################################

#home dir of entrada
export ENTRADA_HOME=""
#tmp dir used for keep state files
export TMP_DIR="$ENTRADA_HOME/tmp/"

#decoder config file
export CONFIG_FILE="../config/entrada-settings.properties"

#Impala deamon hostname for impala-shell to connect to
export IMPALA_NODE=""

#hdfs locations for storing data
export HDFS_HOME=""

#input directories, subdirs must have same name as name server
export DATA_RSYNC_DIR="/home/captures"
#root directory for data input/output
export DATA_DIR=""
#number of days to keep old pcap files
export PCAP_DAYS_TO_KEEP=10

#Log location
export ENTRADA_LOG_DIR="/var/log/entrada"

#name servers, seperate multiple NS with a colon ;
export NAMESERVERS=""

#java
export JAVA_BIN="/usr/lib/jvm/java-7-oracle/bin/java"
export ENTRADA_JAR="pcap-to-parquet-0.0.1-jar-with-dependencies.jar"

#security if Kerberos is enabled, otherwise keep empty
export KRB_USER=user@REALM
export KEYTAB_FILE=""

#error mail recipient
export ERROR_MAIL=""
