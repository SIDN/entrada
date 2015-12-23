#!/usr/bin/env bash

SCRIPT_DIR=$(dirname "$0")
source $SCRIPT_DIR/config.sh

#Change the IP addresses to the correct servers addresses!
HOST_V4=
HOST_V6=
HOSTNAME_PREFIX=

#use  & 0x1fff != 0 to also capture fragmented packets
tcpdump -i eth1 -w ${DUMPDIR}/$HOSTNAME_PREFIX_%Y-%m-%d_%H:%M.pcap.gz -G 300 -z gzip '(( port 53 or ip[6:2] & 0x1fff != 0) or icmp or icmp6 ) and ( host $HOST_V4 or host $HOST_V6 )' 
