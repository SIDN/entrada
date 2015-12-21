#!/usr/bin/env bash

############################################################
#
# Move the received pcap files to the processing directory
# 
############################################################

INCOMING_DIR=$DATA_DIR/incoming
PROCESSING_DIR=$DATA_DIR/processing
SERVER=$1

echo "[$(date)] : start move data for $SERVER"

if [ ! -d "$PROCESSING_DIR/$SERVER" ]; then
   mkdir -p $PROCESSING_DIR/$SERVER
fi

for f in $INCOMING_DIR/$SERVER/*pcap.gz
do
  ! [ -f $f ] && continue

  #check if file is not opened, if not then move file
  if ! [[ `lsof +D $INCOMING_DIR/$SERVER | grep $f` ]]
  then
     file=$(basename $f)
     echo "mv $file $PROCESSING_DIR/$SERVER/$file"
     mv $f $PROCESSING_DIR/$SERVER/$file
  fi
done
echo "[$(date)] : done moving data for $SERVER"
