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
# Boostrap a pcap loader process for each name server
#
############################################################

source entrada-latest/scripts/run/config.sh

PID=$TMP_DIR/run_02_partial_loader_bootstrap

#----- functions ---------------

cleanup(){
  #remove pid file
  if [ -f $PID ];
  then
    sudo rm $PID
  fi

  #edit: since all pcaps are archived in s3 this just takes up extra storage
  if [ -d $DATA_DIR/processed/archive ];
  then
    sudo rm -rf $DATA_DIR/processed/archive
  fi
}

# ------- main program -----------

echo "[$(date)] : Bootstrapping PCAP loading process"

if [ -f $PID ];
then
  echo "[$(date)] : $PID  : Process is already running, do not start new process."
  exit 1
fi

if [ -f $TMP_DIR/import_pcaps ];
then
  echo "[$(date)] : Currently importing pcaps, do not start processing"
  exit 1
fi

#create pid file
sudo echo 1 > $PID

#Make sure cleanup() is called when script is done processing or crashed.
trap cleanup EXIT


CLASS=nl.sidn.pcap.Update

#check if tmp dir exists, if not create
#check if output dir exists
if ! [ -f "$TMP_DIR" ]
then
  mkdir -p $TMP_DIR
fi

#run the update command first, do this here to avoid multiple processen
#doing update parallel.
java -Dentrada_log_dir=$ENTRADA_LOG_DIR -cp $ENTRADA_HOME/$ENTRADA_JAR $CLASS $CONFIG_FILE $TMP_DIR

#Start a loader process for every name server
#If one NS fails for some time to send data there will be a bulk upload later
#if not using seperate processes this bulk upload will cause other NS data
#to delay also.

echo "[$(date)] :Start parallel processing new pcap data"

#parallel will start process for each name server with the same config file
#replace colon with whitespace so it will work with gnu parallel
nslist=$(echo $NAMESERVERS | tr ';' ' ')

#needs ./ prefixing run_02... to work in Amazon Linux
parallel -j $PARALLEL_JOBS ./run_02_partial_loader.sh ::: $nslist ::: $CONFIG_FILE ::: $1

echo "Loaded data for all nameservers"
