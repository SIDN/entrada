#!/usr/bin/env bash

############################################################
##                                                        ##
##                                                        ##
##              ENTRADA configuration                     ##
##                                                        ##
##                                                        ##
############################################################

#home dir of entrada
ENTRADA_HOME=""
#tmp dir used for keep state files
TMP_DIR="$ENTRADA_HOME/tmp/"

#decoder config file
CONFIG_FILE="../config/entrada-settings.properties"

#Impala deamon hostname for impala-shell to connect to
IMPALA_NODE=""

#hdfs locations for storing data
HDFS_HOME=""

#input directories, subdirs must have same name as name server
DATA_RSYNC_DIR="/home/captures"
#root directory for data input/output
DATA_DIR=""

#Log location
ENTRADA_LOG_DIR="/var/log/entrada"

#name servers, seperate multiple NS with a colon ;
NAMESERVERS=""

#java
JAVA_BIN="/usr/lib/jvm/java-7-oracle/bin/java"
ENTRADA_JAR="pcap-to-parquet-0.0.1-jar-with-dependencies.jar"

#security if Kerberos is enabled, otherwise keep empty
KRB_USER=user@REALM
KEYTAB_FILE=""

#error mail recipient
ERROR_MAIL=""
