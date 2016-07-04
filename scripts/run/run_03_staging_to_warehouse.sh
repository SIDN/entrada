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

HDFS_DNS_STAGING="$HDFS_HOME/staging"
HDFS_ICMP_STAGING="$HDFS_HOME/icmp-staging"

IMPALA_DNS_STAGING_TABLE="dns.staging"
IMPALA_ICMP_STAGING_TABLE="icmp.staging"
IMPALA_DNS_DWH_TABLE="dns.queries"
IMPALA_ICMP_DWH_TABLE="icmp.packets"

#use kerberos user "hdfs"
if [ -f $KEYTAB_FILE ];
then
   kinit $KRB_USER -k -t $KEYTAB_FILE
fi

runasImpala(){
  export HADOOP_USER_NAME=impala
}

runasSuperuser(){
  export HADOOP_USER_NAME=hdfs
}

#-----------------------------
# Main script
#-----------------------------

runasImpala

if [ -z "$1" ]
then
  #get date for yesterday
  day=$(date -d "1 days ago" +"%-d")
  year=$(date -d "1 days ago" +"%Y")
  month=$(date -d "1 days ago" +"%-m")
else
  #get date for parameter date, fomat should be YYYY-MM-DD
  day=$(date -d "$1" +"%-d")
  year=$(date -d "$1" +"%Y")
  month=$(date -d "$1" +"%-m")  
fi
echo "[$(date)] : process staging partition: $day-$month-$year"


####
#### DNS data section
####

#insert all the staging data for yesterday into the datawarehouse table
#skip the duplicate "svr" column.
impala-shell -i $IMPALA_NODE -V -q  "insert into $IMPALA_DNS_DWH_TABLE partition(year, month, day, server) select 
     id, unixtime, time, qname, domainname,
     len, frag, ttl, ipv,
     prot, src, srcp, dst,
     dstp, udp_sum, dns_len, aa,
     tc, rd, ra, z, ad, cd,
     ancount, arcount, nscount, qdcount,
     opcode, rcode, qtype, qclass,
     country, asn, edns_udp, edns_version,
     edns_do, edns_ping, edns_nsid, edns_dnssec_dau,
     edns_dnssec_dhu, edns_dnssec_n3u,
     edns_client_subnet, edns_other,
     edns_client_subnet_asn,
     edns_client_subnet_country,
     labels,res_len,time_micro,resp_frag,proc_time,is_google,is_opendns,
     dns_res_len,server_ns_name, server_location, year,month,day,server
     from $IMPALA_DNS_STAGING_TABLE where year=$year and month=$month and day=$day;"

if [ $? -ne 0 ]
then
    #send mail to indicate error
    echo "[$(date)] : insert data into $IMPALA_DNS_DWH_TABLE failed" | mail -s "Impala error" $ERROR_MAIL
    exit 1
fi

echo "[$(date)] : issue refresh for $IMPALA_DNS_DWH_TABLE"
impala-shell --quiet -i $IMPALA_NODE -V -q "refresh $IMPALA_DNS_DWH_TABLE;"
if [ $? -ne 0 ]
then
    #send mail to indicate error
    echo "[$(date)] : refresh metadata $IMPALA_DNS_DWH_TABLE failed" | mail -s "Impala error" $ERROR_MAIL
fi

#delete partitions from staging
for server in $(hdfs dfs -ls $HDFS_DNS_STAGING/year=$year/month=$month/day=$day/ | grep server= | cut -d= -f5)
do
    echo "[$(date)] : drop table $IMPALA_DNS_STAGING_TABLE partition for $server"
    impala-shell -c --quiet -i $IMPALA_NODE -V -q "alter table $IMPALA_DNS_STAGING_TABLE drop partition (year=$year,month=$month,day=$day, server=\"$server\");"
done

#refresh impala metadata
echo "[$(date)] : issue refresh for $IMPALA_DNS_STAGING_TABLE"
impala-shell --quiet -i $IMPALA_NODE -V -q "refresh $IMPALA_DNS_STAGING_TABLE;"
if [ $? -ne 0 ]
then
     #send mail to indicate error
     echo "[$(date)] : refresh metadata $IMPALA_DNS_STAGING_TABLE failed" | mail -s "Impala error" $ERROR_MAIL
fi

#get new rows count
rows=$(impala-shell --quiet -i $IMPALA_NODE -q "select count(1) from $IMPALA_DNS_DWH_TABLE where year=$year and month=$month and day=$day;" --quiet -B)
echo "[$(date)] : added $rows new rows to $IMPALA_DNS_DWH_TABLE"
   
#delete staging data from hdfs
runasSuperuser
echo "[$(date)] : delete the staging parquet files from hdfs $HDFS_DNS_STAGING/year=$year/month=$month/day=$day"
hdfs dfs -rm -r -f $HDFS_DNS_STAGING/year=$year/month=$month/day=$day
runasImpala

#update the table statistics
impala-shell -c --quiet -i $IMPALA_NODE -q "COMPUTE INCREMENTAL STATS $IMPALA_DNS_DWH_TABLE;" 
 
####
#### ICMP data section
####

#skip the duplicate "svr" column.
impala-shell -i $IMPALA_NODE -V -q  "insert into $IMPALA_ICMP_DWH_TABLE partition(year, month, day) select 
      unixtime,icmp_type,
      icmp_code,icmp_echo_client_type,ip_ttl,
      ip_v,ip_src,
      ip_dst,ip_country,
      ip_asn,ip_len,
      l4_prot,l4_srcp,
      l4_dstp,orig_ip_ttl,
      orig_ip_v,orig_ip_src,
      orig_ip_dst,orig_l4_prot,
      orig_l4_srcp,orig_l4_dstp,
      orig_udp_sum,orig_ip_len,
      orig_icmp_type,orig_icmp_code,
      orig_icmp_echo_client_type,
      orig_dns_id,orig_dns_qname,
      orig_dns_domainname,orig_dns_len,
      orig_dns_aa,orig_dns_tc,
      orig_dns_rd,orig_dns_ra,
      orig_dns_z,orig_dns_ad,
      orig_dns_cd,orig_dns_ancount,
      orig_dns_arcount,orig_dns_nscount,
      orig_dns_qdcount,orig_dns_rcode,
      orig_dns_qtype,orig_dns_opcode,
      orig_dns_qclass,orig_dns_edns_udp,
      orig_dns_edns_version,orig_dns_edns_do,
      orig_dns_labels,svr,time_micro,server_ns_name, server_location, year,month,day
     from $IMPALA_ICMP_STAGING_TABLE where year=$year and month=$month and day=$day;"

if [ $? -ne 0 ]
then
    #send mail to indicate error
    echo "[$(date)] : insert data into $IMPALA_ICMP_DWH_TABLE failed" | mail -s "Impala error" $ERROR_MAIL
    exit 1
fi

echo "[$(date)] : issue refresh for $IMPALA_ICMP_DWH_TABLE"
impala-shell --quiet -i $IMPALA_NODE -V -q "refresh $IMPALA_ICMP_DWH_TABLE;"
if [ $? -ne 0 ]
then
     #send mail to indicate error
     echo "[$(date)] : refresh metadata $IMPALA_ICMP_DWH_TABLE failed" | mail -s "Impala error" $ERROR_MAIL
fi


#delete partition from staging
impala-shell -c --quiet -i $IMPALA_NODE -V -q "alter table $IMPALA_ICMP_STAGING_TABLE drop partition (year=$year,month=$month,day=$day);"

#refresh impala metadata
echo "[$(date)] : issue refresh for $IMPALA_ICMP_STAGING_TABLE"
impala-shell --quiet -i $IMPALA_NODE -V -q "refresh $IMPALA_ICMP_STAGING_TABLE;"
if [ $? -ne 0 ]
then
     #send mail to indicate error
     echo "[$(date)] : refresh metadata $IMPALA_ICMP_STAGING_TABLE failed" | mail -s "Impala error" $ERROR_MAIL
fi

#get new rows count
rows=$(impala-shell --quiet -i $IMPALA_NODE -q "select count(1) from $IMPALA_ICMP_DWH_TABLE where year=$year and month=$month and day=$day;" --quiet -B)
echo "[$(date)] : added $rows new rows to $IMPALA_ICMP_DWH_TABLE"
   
#delete staging data from hdfs
runasSuperuser
echo "[$(date)] : delete the staging parquet files from hdfs $HDFS_ICMP_STAGING/year=$year/month=$month/day=$day"
hdfs dfs -rm -r -f $HDFS_ICMP_STAGING/year=$year/month=$month/day=$day
runasImpala

#update the table statistics
impala-shell --quiet -i $IMPALA_NODE -q "COMPUTE INCREMENTAL STATS $IMPALA_ICMP_DWH_TABLE;" 
   
echo "[$(date)] : done moving data to warehouse"


