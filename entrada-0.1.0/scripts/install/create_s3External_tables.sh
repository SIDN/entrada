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

for f in $SCRIPT_DIR/../database/*.sql
do
    script=$(< $f)
    #replace hdfs root placeholder
    script=${script/_HDFS_LOCATION_/$HDFS_HOME}

    # split the table name into db and table names and replace the placeholders
    #edit: removed ICMP parts
    for TABLE in IMPALA_DNS_STAGING_TABLE IMPALA_DNS_DWH_TABLE
    do
        IFS='.';
        array=(${!TABLE})
        unset IFS;
        DB=${array[0]}
        TAB=${array[1]}

        script=${script//_${TABLE}_DB_/$DB}
        script=${script//_${TABLE}_TAB_/$TAB}
    done
    for LOC in S3_DNS_STAGING S3_DNS_QUERIES
    do
        script=${script//_${LOC}_/${!LOC}}
    done
    hive -e "$script"
done
