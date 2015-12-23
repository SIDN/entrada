#!/usr/bin/env bash

############################################################
#
# delete old pcap files
# 
############################################################

find $DATA_DIR/processed/archive/**/*.pcap.gz -mtime +$PCAP_DAYS_TO_KEEP -exec rm {} \;
