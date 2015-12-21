#!/usr/bin/env bash

############################################################
#
# Move the received pcap files to the processing directory
# 
############################################################

SCRIPT_DIR=$(dirname "$0")
source $SCRIPT_DIR/config.sh

nsarray=(`echo $NAMESERVERS | tr ';' ' '`)

for ns in ${nsarray[@]}; do
  $SCRIPT_DIR/run_01_move_to_processing.sh $ns
done

