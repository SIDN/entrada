#!/usr/bin/env bash

############################################################
#
# Boostrap a pcap loader process for each name server
# 
############################################################

CLASS=nl.sidn.pcap.Update
#PATH=$PATH:$SCRIPT_DIR:/usr/local/bin
#CONFIG_FILE=$SCRIPT_DIR/../config/entrada-settings.properties

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

parallel run_02_partial_loader.sh ::: $nslist ::: $CONFIG_FILE



