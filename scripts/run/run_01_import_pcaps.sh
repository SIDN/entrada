#!/usr/bin/env bash

# This bash script is made to move incoming pcap files from S3 to local storage
# and then copy those files onto an archive built in S3.

source entrada-latest/scripts/run/config.sh

# INPUT_INCLUDE=\"*.pcap\"
# INPUT_EXCLUDE=\"*\"
SOURCE="$S3_HOME/input"
OUTPUT_DIR="$DATA_DIR/processing"
ARCHIVE="$S3_ARCHIVE"
PID=$TMP_DIR/import_pcaps

# copied with modifications from the original scripts:
cleanup(){
  #remove pid file
  if [ -f $PID ];
  then
    sudo rm $PID
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
sudo echo 1 > $PID

#Make sure cleanup() is called when script is done processing or crashed.
trap cleanup EXIT
#

echo "[$(date)] : Starting download from $S3_HOME/input"
fileCount=$(aws s3 mv $SOURCE $OUTPUT_DIR --recursive --no-progress | grep 'pcap' | wc -l)
if [ $fileCount -eq 0 ]
then
    #if no files where downloaded exit with error which will cause processing.sh
    #to exit as well
    exit 1
fi
echo "[$(date)] : Finished downloading $fileCount files"

echo "[$(date)] : Starting archivation of pcaps to $ARCHIVE"
aws s3 cp $OUTPUT_DIR $ARCHIVE --recursive
echo "[$(date)] : Archivation complete"

echo "[$(date)] : PCAP import complete"
