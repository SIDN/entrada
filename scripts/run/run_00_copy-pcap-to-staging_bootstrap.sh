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

parallel run_00_copy-pcap-to-staging.sh  ::: $nslist
#$SCRIPT_DIR/run_00_copy-pcap-to-staging.sh "ns2.dns.nl"