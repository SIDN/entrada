#!/usr/bin/env bash

############################################################
#
# delete old pcap files
# 
############################################################

SCRIPT_DIR=$(dirname "$0")
source $SCRIPT_DIR/config.sh

DAYS_TO_KEEP=10

find $DATA_DIR/processed/archive/**/*.gz -mtime +$DAYS_TO_KEEP -exec rm {} \;
