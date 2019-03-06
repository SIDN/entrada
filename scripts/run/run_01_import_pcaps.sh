#!/usr/bin/env bash

# This bash script is made to move incoming pcap files from S3 to local storage
# and then copy those files onto an archive built in S3.

# INPUT_INCLUDE=\"*.pcap\"
# INPUT_EXCLUDE=\"*\"
SOURCE="s3://$S3_HOME/input"
OUTPUT_DIR="$DATA_DIR/processing"
ARCHIVE="$S3_ARCHIVE"
PID=$TMP_DIR/import_pcaps

# copied with modifications from the original scripts:
cleanup(){
  #remove pid file
  if [ -f $PID ];
  then
    rm $PID
  fi
}

echo "[$(date)] : Starting PCAP import"

if [ -f $PID ];
then
  echo "[$(date)] : $PID  : Process is already running, do not start new process."
  exit 1
fi

#check if tmp dir exists, if not create it
if ! [ -f "$TMP_DIR" ]
then
  mkdir -p $TMP_DIR
fi

#create pid file
echo 1 > $PID

#Make sure cleanup() is called when script is done processing or crashed.
trap cleanup EXIT
#

echo "[$(date)] : Starting download from $S3_HOME/input"
aws s3 mv $SOURCE $OUTPUT_DIR --recursive
echo "[$(date)] : Finished downloading $(ls ./pcap/processing/*/*.* 2>/dev/null | wc -l) files"

echo "[$(date)] : Starting archivation of pcaps to $ARCHIVE"
aws s3 cp $OUTPUT_DIR $ARCHIVE --recursive
echo "[$(date)] : Archivation complete"

echo "[$(date)] : PCAP import complete"
