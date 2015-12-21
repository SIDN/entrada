#!/usr/bin/env bash

############################################################
#
# Copy the received pcap files to the processing directory
# 
############################################################

SCRIPT_DIR=$(dirname "$0")
source $SCRIPT_DIR/config.sh

INCOMING_DIR=$DATA_DIR/incoming
PROCESSING_DIR=$DATA_DIR/processing
SERVER=$1

if [ ! -d "$PROCESSING_DIR/$SERVER" ]; then
   mkdir -p $PROCESSING_DIR/$SERVER
fi

for f in $INCOMING_DIR/$SERVER/*pcap.gz
do
  echo "Processing $f"
   ! [ -f $f ] && continue

  #check if file is not opened, if not then copy file
  if ! [[ `lsof +D $INCOMING_DIR/$SERVER | grep $f` ]]
  then
     file=$(basename $f)
     echo "cp $file $PROCESSING_DIR/$SERVER/$file"
     cp $f $PROCESSING_DIR/$SERVER/$file
  fi
done
