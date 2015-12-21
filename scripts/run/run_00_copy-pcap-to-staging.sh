#!/usr/bin/env bash

############################################################
#
# Copy the new pcap files to the input location
# 
############################################################

NAMESERVER=$1
HISTORY_FILE="$TMP_DIR/$NAMESERVER-pcap-process.hist"
INPUT_FILTER="*.pcap.gz"
INPUT_DIR="$DATA_RSYNC_DIR/$NAMESERVER"
OUTPUT_DIR="$DATA_DIR/incoming/$NAMESERVER"
PID=$TMP_DIR/run_00_copy-pcap-to-staging_$NAMESERVER

#----- functions ---------------

cleanup(){
  #remove pid file
  if [ -f $PID ];
  then
     rm $PID
  fi 
}

# ------- main program -----------

echo "[$(date)] : start copy data for $NAMESERVER"

if [ -f $PID ];
then
   echo "[$(date)] : $PID  : Process is already running, do not start new process."
   exit 1
fi

#create pid file
echo 1 > $PID

#Make sure cleanup() is called when script is done processing or crashed.
trap cleanup EXIT

#check if hist file exists, if not create it
if ! [ -f "$HISTORY_FILE" ]
then
  touch $HISTORY_FILE
fi

#check if output dir exists
if ! [ -f "$OUTPUT_DIR" ]
then
  mkdir -p $OUTPUT_DIR
fi

echo "[$(date)] : History will be saved in $HISTORY_FILE"

count=0
#loop through all *.pcap files
#Skip the newest, it might still be written to
files=($( ls -t $INPUT_DIR/$INPUT_FILTER |tail -n +2 ))
fcount=${#files[@]}
echo "[$(date)] : found $fcount files"

for (( i = 0 ; i < $fcount ; i++))
do
    f=${files[$i]}
    #check if it is a file
    if ! [ -f "$f" ]
    then
      echo "[$(date)] : warning $f is not a valid file"
      continue
    fi

    #check if the file is not allready processed
    if ! [[ $( grep $f $HISTORY_FILE) ]]
    then
        echo "[$(date)] : cp $f -> $OUTPUT_DIR"
        cp $f $OUTPUT_DIR && count=$((count+1)) && echo $f >> $HISTORY_FILE
    fi
done
echo "[$(date)] : end, copied $count files."
