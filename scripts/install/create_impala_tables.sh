#!/usr/bin/env bash

##########################################################################
#
# Create DNS and ICMP staging and warehouse tables
#
##########################################################################

SCRIPT_DIR=$(dirname "$0")
source $SCRIPT_DIR/../config.sh

#use kerberos user "hdfs"
if [ -f $KEYTAB_FILE ];
then
   echo "init kerb"
   kinit $KRB_USER -k -t $KEYTAB_FILE
fi

for f in ../database/*.sql
do
    #create dns staging table
    script=$(< $f)
    script=${script/_HDFS_LOCATION_/$HDFS_HOME}
    impala-shell -k -i $IMPALA_NODE -V -q  "$script"
done


#invalidate metadata
impala-shell -k -i $IMPALA_NODE -V -q  "invalidate metadata;"


