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
# Update statistics for staging tables
# 
############################################################

#use kerberos user "hdfs"
if [ -f "$KEYTAB_FILE" ];
then
   kinit $KRB_USER -k -t $KEYTAB_FILE
fi

#run all hdfs actions as user impala
export HADOOP_USER_NAME=impala


echo "[$(date)] : Update stats for staging"
impala-shell -i $IMPALA_NODE -q "COMPUTE STATS $IMPALA_DNS_STAGING_TABLE;"
if [ $? -ne 0 ]
then
   #send mail to indicate error
   echo "[$(date)] : Update stats $IMPALA_DNS_STAGING_TABLE failed" | mail -s "Impala error updating staging stats" $ERROR_MAIL
fi

impala-shell -i $IMPALA_NODE -q "COMPUTE STATS $IMPALA_ICMP_STAGING_TABLE;"
if [ $? -ne 0 ]
then
   #send mail to indicate error
   echo "[$(date)] : Update stats $IMPALA_ICMP_STAGING_TABLE failed" | mail -s "Impala error updating staging stats" $ERROR_MAIL
fi



