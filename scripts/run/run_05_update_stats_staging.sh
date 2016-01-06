#!/usr/bin/env bash

############################################################
#
# Update statistics for staging tables
# 
############################################################

IMPALA_DNS_STAGING_TABLE="dns.staging"
IMPALA_ICMP_STAGING_TABLE="icmp.staging"

#use kerberos user "hdfs"
if [ -f $KEYTAB_FILE ];
then
   kinit $KRB_USER -k -t $KEYTAB_FILE
fi

#run all hdfs actions as user impala
export HADOOP_USER_NAME=impala


echo "[$(date)] : Update stats for staging"
impala-shell -i $IMPALA_NODE -q "COMPUTE STATS $IMPALA_DNS_STAGING_TABLE;"
if [ $? -ne 0 ]
then
   #send mail to indicate error
   echo "[$(date)] : Update stats $IMPALA_DNS_STAGING_TABLE failed" | mail -s "Impala error updating staging stats" $ERROR_MAIL
fi

impala-shell -i $IMPALA_NODE -q "COMPUTE STATS $IMPALA_ICMP_STAGING_TABLE;"
if [ $? -ne 0 ]
then
   #send mail to indicate error
   echo "[$(date)] : Update stats $IMPALA_ICMP_STAGING_TABLE failed" | mail -s "Impala error updating staging stats" $ERROR_MAIL
fi



