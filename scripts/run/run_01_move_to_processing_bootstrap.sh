#!/usr/bin/env bash

############################################################
#
# Move the received pcap files to the processing directory
# 
############################################################

nsarray=(`echo $NAMESERVERS | tr ';' ' '`)

for ns in ${nsarray[@]}; do
  $SCRIPT_DIR/run_01_move_to_processing.sh $ns
done

