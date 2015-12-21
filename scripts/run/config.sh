#!/usr/bin/env bash

############################################################
##                                                        ##
##                                                        ##
##              ENTRADA configuration                     ##
##                                                        ##
##                                                        ##
############################################################

#home dir of entrada
ENTRADA_HOME="/staging/loader/logic/"
#tmp dir used for keep state files
TMP_DIR="$ENTRADA_HOME/tmp/"

#Impala deamon hostname for impala-shell to connect to
IMPALA_NODE="hadoop-data-01.sidnlabs.nl"

#hdfs locations for storing data
HDFS_HOME="/user/hive/"

#input directories, subdirs must have same name as name server
DATA_RSYNC_DIR="/home/captures"
#output directory root
DATA_DIR="/staging/pcap"

#Log location
ENTRADA_LOG_DIR="/var/log/entrada"

#name servers, seperate multiple NS with a colon ;
NAMESERVERS="ns1.dns.nl;ns2.dns.nl"

#java
JAVA_BIN="/usr/lib/jvm/java-7-oracle/bin/java"
ENTRADA_JAR="pcap-to-parquet-1.0.0-jar-with-dependencies.jar"

#security if Kerberos is enabled, otherwise keep empty
KRB_USER=hdfs@SIDNLABS
KEYTAB_FILE="/home/loadmaster/hdfs_keytab"

#error mail recipient
ERROR_MAIL="entrada@sidn.nl"
