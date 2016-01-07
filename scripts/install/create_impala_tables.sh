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

##########################################################################
#
# Create DNS and ICMP staging and warehouse tables
#
##########################################################################

SCRIPT_DIR=$(dirname "$0")
source $SCRIPT_DIR/../config.sh

#use kerberos user "hdfs"
if [ -f $KEYTAB_FILE ];
then
   echo "init kerb"
   kinit $KRB_USER -k -t $KEYTAB_FILE
fi

for f in ../database/*.sql
do
    script=$(< $f)
    #replace hdfs root placeholder
    script=${script/_HDFS_LOCATION_/$HDFS_HOME}
    impala-shell -i $IMPALA_NODE -V -q  "$script"
done


#invalidate metadata
impala-shell -k -i $IMPALA_NODE -V -q  "invalidate metadata;"


