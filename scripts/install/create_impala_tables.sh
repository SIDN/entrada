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
echo "SCRIPT_DIR == $SCRIPT_DIR"
source $SCRIPT_DIR/../run/config.sh

IMPALA_OPTS=

if [ -f "$KEYTAB_FILE" ];
then
   echo "initialize kerberos ticket"
   kinit $KRB_USER -k -t $KEYTAB_FILE
   IMPALA_OPTS=-k
fi

for f in $SCRIPT_DIR/../database/*.sql
do
    script=$(< $f)
    #replace hdfs root placeholder
    script=${script/_HDFS_LOCATION_/$HDFS_HOME}

    # split the table name into db and table names and replace the placeholders
    for TABLE in IMPALA_DNS_STAGING_TABLE IMPALA_ICMP_STAGING_TABLE IMPALA_DNS_DWH_TABLE IMPALA_ICMP_DWH_TABLE
    do
        IFS='.';
        array=(${!TABLE})
        unset IFS;
        DB=${array[0]}
        TAB=${array[1]}

        script=${script//_${TABLE}_DB_/$DB}
        script=${script//_${TABLE}_TAB_/$TAB}
    done
    for LOC in HDFS_DNS_STAGING HDFS_ICMP_STAGING HDFS_DNS_QUERIES HDFS_ICMP_PACKETS
    do 
        script=${script//_${LOC}_/${!LOC}}
    done
    impala-shell $IMPALA_OPTS -i $IMPALA_NODE -V -q  "$script"
done

#invalidate metadata
impala-shell $IMPALA_OPTS -i $IMPALA_NODE -V -q  "invalidate metadata;"


