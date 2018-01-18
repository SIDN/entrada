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

############################################################
#
# Move the received pcap files to the processing directory
#
############################################################

INCOMING_DIR=$DATA_DIR/incoming
PROCESSING_DIR=$DATA_DIR/processing
SERVER=$1

echo "[$(date)] : start move data for $SERVER"

if [ ! -d "$PROCESSING_DIR/$SERVER" ]; then
   mkdir -p $PROCESSING_DIR/$SERVER
fi

for f in $INCOMING_DIR/$SERVER/*
do
  ! [ -f $f ] && continue

  #check if file is not opened, if not then move file
  if ! [[ `lsof +D $INCOMING_DIR/$SERVER | grep $f` ]]
  then
     file=$(basename $f)
     echo "[$(date)] : move $file $PROCESSING_DIR/$SERVER/$file"
     mv $f $PROCESSING_DIR/$SERVER/$file
  fi
done
echo "[$(date)] : done moving data for $SERVER"
