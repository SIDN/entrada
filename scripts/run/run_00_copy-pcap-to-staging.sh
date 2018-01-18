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
# Copy the new pcap files to the input location
#
############################################################

NAMESERVER=$1
HISTORY_FILE="$TMP_DIR/$NAMESERVER-pcap-process.hist"
INPUT_FILTER="*.pcap*"
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

echo "[$(date)] : start copy data for $NAMESERVER in dir: $INPUT_DIR"

if [ -f $PID ];
then
   echo "[$(date)] : $PID  : Process is already running, do not start new process."
   exit 1
fi

#create pid file
echo 1 > $PID

#Make sure cleanup() is called when script is done processing or crashed.
trap cleanup EXIT

#check if tmp dir exists, if not create it
if ! [ -f "$TMP_DIR" ]
then
  mkdir -p $TMP_DIR
fi

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
echo "[$(date)] : found $fcount files to check"

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
        if [ "$DELETE_INPUT_PCAP_FILES" = true ] ; then
            echo "[$(date)] : rm $f"
            rm $f
        fi
    fi
done
echo "[$(date)] : end, copied $count files."
