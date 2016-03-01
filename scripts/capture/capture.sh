#!/usr/bin/env bash

# ENTRADA, a big data platform for network data analytics
# 
# Copyright (C) 2016 SIDN [https://www.sidn.nl]
#  
# This file is part of ENTRADA.
# 
# ENTRADA is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# ENTRADA is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#  
# You should have received a copy of the GNU General Public License
# along with ENTRADA.  If not, see [<http://www.gnu.org/licenses/].


SCRIPT_DIR=$(dirname "$0")
source $SCRIPT_DIR/config.sh

#Change the IP addresses to the correct servers addresses!
HOST_V4=
HOST_V6=
HOSTNAME_PREFIX=

#use  & 0x1fff != 0 to also capture fragmented packets
tcpdump -i eth1 -w ${DUMPDIR}/$HOSTNAME_PREFIX_%Y-%m-%d_%H:%M.pcap -G 300 -z gzip "(( port 53 or ip[6:2] & 0x1fff != 0) or icmp or icmp6 ) and ( host $HOST_V4 or host $HOST_V6 )"
