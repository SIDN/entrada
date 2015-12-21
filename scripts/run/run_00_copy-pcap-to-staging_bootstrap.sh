#!/usr/bin/env bash

############################################################
#
# Copy the new pcap files to the correct input directory
# 
############################################################

SCRIPT_DIR=$(dirname "$0")
source $SCRIPT_DIR/config.sh

PATH=$PATH:$SCRIPT_DIR:/usr/local/bin

#parallel will start process for each name server
nslist=$(echo $NAMESERVERS | tr ';' ' ')
echo "copy data for: $NAMESERVERS"

parallel run_00_copy-pcap-to-staging.sh  ::: $nslist