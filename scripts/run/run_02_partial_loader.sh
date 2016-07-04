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
#
# Convert pcap files to parquet files
# 
############################################################

CLASS="nl.sidn.pcap.Main"
OUTPUT_DIR="$DATA_DIR/processed"
HDFS_DNS_STAGING="$HDFS_HOME/staging"
HDFS_ICMP_STAGING="$HDFS_HOME/icmp-staging"

IMPALA_DNS_STAGING_TABLE="dns.staging"
IMPALA_ICMP_STAGING_TABLE="icmp.staging"

#use kerberos user "hdfs"
if [ -f $KEYTAB_FILE ];
then
   kinit $KRB_USER -k -t $KEYTAB_FILE
fi

#run all hdfs actions as user impala
export HADOOP_USER_NAME=impala

NAMESERVER=$1
CONFIG_FILE=$2

if [ -z "$3" ]
then
  #get date for today
  DAY=$(date +"%-d")
  YEAR=$(date +"%Y")
  MONTH=$(date +"%-m")
else
  #get date for parameter date, fomat should be YYYY-MM-DD
  DAY=$(date -d "$3" +"%-d")
  YEAR=$(date -d "$3" +"%Y")
  MONTH=$(date -d "$3" +"%-m")
fi

echo "[$(date)] :Start loader for $NAMESERVER with config $CONFIG_FILE with date $YEAR-$MONTH-$DAY"

#kitesdk naming workaround
#replace "." by an "_" because kitesdk does not support dots in the namespace
#see: https://issues.cloudera.org/browse/KITE-673
NORMALIZED_NAMESERVER=${NAMESERVER//./_}
echo "[$(date)] :Normalized name server: $NORMALIZED_NAMESERVER"

PID=$TMP_DIR/run_02_partial_loader_$NAMESERVER

#-------------------------------
# Helper functions
#------------------------------

cleanup(){
  if [ -d $OUTPUT_DIR/$NORMALIZED_NAMESERVER ];
  then
      echo "rm -rf $OUTPUT_DIR/$NORMALIZED_NAMESERVER"
      rm -rf $OUTPUT_DIR/$NORMALIZED_NAMESERVER 
  fi

  #remove pid file
  rm $PID 
}

remove_zeroes(){
    for year_dir in */
    do
        #goto year
        cd $year_dir
        #rename months, remove leading zeros
        for subdir in month=0*; do mv "${subdir}" "${subdir/0/}"; done 2>/dev/null
        for month_dir in month=*/
        do
            #goto month dir
            cd $month_dir
            #rename days, remove leading zeros
            for subdir in day=0*; do mv "${subdir}" "${subdir/0/}"; done 2>/dev/null
            cd ..
        done
        cd ..
    done
}

#-----------------------------
# Main script
#-----------------------------

if [ -f $PID ];
then
   echo "[$(date)] : $PID  : Process is already running, do not start new process."
   exit 0
fi

#create pid file
echo 1 > $PID

#Make sure cleanup() is called when script is done processing or crashed.
trap cleanup EXIT

#check if hdfs data location exists
hdfs dfs -test -d "$HDFS_DNS_STAGING" 
if [ $? -ne "0" ]
then
   echo "[$(date)] : hdfs root path $HDFS_DNS_STAGING does not exist, stop"
   exit 0
fi

#transform pcap data to parquet data
java -Dentrada_log_dir=$ENTRADA_LOG_DIR -cp $ENTRADA_HOME/$ENTRADA_JAR $CLASS $NAMESERVER $CONFIG_FILE $DATA_DIR/processing $DATA_DIR/processed $TMP_DIR
#if Java process exited ok, continue
if [ $? -eq 0 ]
then

   #check if parquet files were created
   if [ ! -d "$OUTPUT_DIR/$NORMALIZED_NAMESERVER/dnsdata" ];
   then
     echo "[$(date)] :No parquet files generated, quit script"
     exit 0
   fi

   #goto location of created parquet files
   cd $OUTPUT_DIR/$NORMALIZED_NAMESERVER 

   #delete useless meta data
   echo  "[$(date)] :delete .crc and  .tmp files"
   find . -name '.*.crc' -print0 | xargs -0 --no-run-if-empty rm
   find . -name '.*.tmp' -print0 | xargs -0 --no-run-if-empty rm

   ####
   #### DNS data section
   ####

   #fix date partition format, remove leading zero otherwize impala partions with int type will not work
   #at the start of the new year there may be 2 distinct years in the data.
   cd dnsdata
   remove_zeroes

   #check if partition exists
   isPartitioned=$(impala-shell -B --quiet -i $IMPALA_NODE -q "select count(1) from $IMPALA_DNS_STAGING_TABLE where year=$YEAR and month=$MONTH and day=$DAY and server=\"$NAMESERVER\";" )

   echo "[$(date)] :upload the parquet files to hdfs $HDFS_DNS_STAGING"
   hdfs dfs -D dfs.block.size=268435456 -put year\=* $HDFS_DNS_STAGING
   #make sure the permissions are set ok
   hdfs dfs -chown -R impala:hive $HDFS_DNS_STAGING/year\=$YEAR/month\=$MONTH/day\=$DAY

   if  [[ $isPartitioned -eq  0 ]]
   then
      echo "[$(date)] :Create Impala partition for year=$YEAR , month=$MONTH and day=$DAY"
      impala-shell -c --quiet -i $IMPALA_NODE -V -q "alter table $IMPALA_DNS_STAGING_TABLE add partition (year=$YEAR,month=$MONTH,day=$DAY,server=\"$NAMESERVER\");"
      if [ $? -ne 0 ]
      then
        #the partition probably already exists
        echo "[$(date)] :Adding partition to table $IMPALA_DNS_STAGING_TABLE failed"
      fi
   else
     echo "[$(date)] :Partition for $YEAR/$MONTH/$DAY already exists"
   fi

   echo "[$(date)] :Issue refresh"
   impala-shell --quiet -i $IMPALA_NODE --quiet -q "refresh $IMPALA_DNS_STAGING_TABLE;"
   if [ $? -ne 0 ]
   then
     #send mail to indicate error
     echo "[$(date)] :Refresh metadata $IMPALA_DNS_STAGING_TABLE failed" | mail -s "Impala error" $ERROR_MAIL
   fi

   ####
   #### ICMP data section
   ####

   #fix date partition format, remove leading zero otherwize impala partions with int type will not work
   cd ../icmpdata
   remove_zeroes

   #check if partition exists
   isPartitioned=$(impala-shell -B --quiet  -i $IMPALA_NODE -q "select count(1) from $IMPALA_ICMP_STAGING_TABLE where day=$DAY;" )

   echo "[$(date)] :upload the parquet files to hdfs $HDFS_ICMP_STAGING"
   hdfs dfs -D dfs.block.size=268435456 -put year\=* $HDFS_ICMP_STAGING
   #make sure the permissions are set ok
   hdfs dfs -chown -R impala:hive $HDFS_ICMP_STAGING/year=$YEAR/month=$MONTH/day=$DAY

   if  [[ $isPartitioned -eq  0 ]]
   then
      echo "[$(date)] :Create Impala partition for year=$YEAR , month=$MONTH and day=$DAY"
      impala-shell -c --quiet -i $IMPALA_NODE -V -q "alter table $IMPALA_ICMP_STAGING_TABLE add partition (year=$YEAR,month=$MONTH,day=$DAY);"
      if [ $? -ne 0 ]
      then
        #the partition probably already exists
        echo "[$(date)] :Adding partition to table $IMPALA_ICMP_STAGING_TABLE failed"
      fi
   else
     echo "[$(date)] :Partition for $YEAR/$MONTH/$DAY already exists"
   fi
   echo "[$(date)] :Issue refresh"
   impala-shell --quiet -i $IMPALA_NODE --quiet -q "refresh $IMPALA_ICMP_STAGING_TABLE;"
   if [ $? -ne 0 ]
   then
     #send mail to indicate error
     echo "[$(date)] :Refresh metadata $IMPALA_ICMP_STAGING_TABLE failed" | mail -s "Impala error" $ERROR_MAIL
   fi
else
   echo "[$(date)] :Converting pcap to parquet failed"
fi
echo "[$(date)] :Done with loading data into staging"


