#!/usr/bin/env bash

SCRIPT_DIR=$(dirname "$0")
source $SCRIPT_DIR/config.sh

#Change the IP addresses to the correct servers addresses!
HOST_V4=193.176.144.5
HOST_V6=2a00:d78:0:102:193:176:144:5
HOSTNAME_PREFIX=ns1

#use  & 0x1fff != 0 to also capture fragmented packets
tcpdump -i eth1 -w ${DUMPDIR}/$HOSTNAME_PREFIX_%Y-%m-%d_%H:%M.pcap -G 300 '(( port 53 or ip[6:2] & 0x1fff != 0) or icmp or icmp6 ) and ( host $HOST_V4 or host $HOST_V6 )' 
