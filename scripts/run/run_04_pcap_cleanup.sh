#!/usr/bin/env bash

############################################################
#
# delete old pcap files
# 
############################################################

DAYS_TO_KEEP=10

find $DATA_DIR/processed/archive/**/*.gz -mtime +$DAYS_TO_KEEP -exec rm {} \;
