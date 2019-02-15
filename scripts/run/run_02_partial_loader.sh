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

#run all hdfs actions as user impala #edit: not using impala so use hdfs as user name instead
#export HADOOP_USER_NAME=impala
export HADOOP_USER_NAME=hdfs

NAMESERVER=$1
CONFIG_FILE=$2

echo "[$(date)] :Start loader for $NAMESERVER with config $CONFIG_FILE"

#kitesdk naming workaround
#replace "." by an "_" because kitesdk does not support dots in the namespace
#see: https://issues.cloudera.org/browse/KITE-673
#NORMALIZED_NAMESERVER=${NAMESERVER//./_}
NORMALIZED_NAMESERVER=$(echo $NAMESERVER | sed 's/[^a-zA-Z0-9]/_/g')
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
    if [ -d "$year_dir" ]; then
      cd $year_dir
      #rename months, remove leading zeros
      for subdir in month=0*; do mv "${subdir}" "${subdir/0/}"; done 2>/dev/null
      for month_dir in month=*/
      do
        #goto month dir
        if [ -d "$month_dir" ]; then
          cd $month_dir
          #rename days, remove leading zeros
          for subdir in day=0*; do mv "${subdir}" "${subdir/0/}"; done 2>/dev/null
          cd ..
        fi
      done
      cd ..
    fi
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

#edit: check if s3 data location exists
hdfs dfs -test -d "$S3_DNS_STAGING"
if [ $? -ne "0" ]
then
  echo "[$(date)] : hdfs root path $S3_DNS_STAGING does not exist, stop"
  exit 0
fi

#transform pcap data to parquet data
java -Xms$ENTRADA_HEAP_SIZE -Xmx$ENTRADA_HEAP_SIZE -Dentrada_log_dir=$ENTRADA_LOG_DIR -cp $ENTRADA_HOME/$ENTRADA_JAR $CLASS $NAMESERVER $CONFIG_FILE $DATA_DIR/processing $DATA_DIR/processed $TMP_DIR
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

  #fix date partition format, remove leading zero otherwize impala partions with int type will not work
  #at the start of the new year there may be 2 distinct years in the data.
  cd dnsdata
  remove_zeroes

  echo "[$(date)] :upload the parquet files to hdfs $S3_DNS_STAGING"

  #edit: recursively move all directories and files into the staging folder on S3
  aws s3 mv --recursive ./ $S3_DNS_STAGING/
  #edit: automatically detect any new partitions as long as the naming convention of the parent folders is correct
  hive -e "MSCK REPAIR TABLE $DNS_STAGING_TABLE"

  #edit: removed the original part which did the move to staging entirely since s3 is used instead of hdfs for storage

  #edit: this fails in hive, should be looked into if its necessary or not
  # echo "[$(date)] :Issue refresh"
  # hive -S -e "refresh $DNS_STAGING_TABLE;"
  # if [ $? -ne 0 ]
  # then
  #   #send mail to indicate error
  #   echo "[$(date)] :Refresh metadata $DNS_STAGING_TABLE failed" | mail -s "Impala error" $ERROR_MAIL
  # fi
else
  echo "[$(date)] :Converting pcap to parquet failed"
fi
echo "[$(date)] :Done with loading data into staging"
