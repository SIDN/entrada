#!/usr/bin/env bash

##########################################################################
#
# Privacy module implementation for ENTRADA.
# IP addresses will be removed from data older than 18 months.
#
##########################################################################

SCRIPT_DIR=$(dirname "$0")
source $SCRIPT_DIR/config.sh

#max age of data before it is anonymized
MAG_AGE_MONTH=18
#impala temp table 
IMPALA_DNS_TMP_DIR="$HDFS_HOME/queries_anon_export"
IMPALA_TMP_TABLE="dns.queries_anon_export"
HDFS_DNS_QUERIES="$HDFS_HOME/queries"

IMPALA_DNS_DWH_TABLE="dns.queries"

#use kerberos user "hdfs"
if [ -f $KEYTAB_FILE ];
then
   kinit $KRB_USER -k -t $KEYTAB_FILE
fi

#do not pad month and day with zero 
start_date=$(date --date="-$MAG_AGE_MONTH month" +"%Y-%-m-%-d")
echo "Anonymize data older than: $start_date"

file_year=$(echo $start_date | cut -d"-" -f 1)
file_month=$(echo $start_date | cut -d"-" -f 2)
file_day=$(echo $start_date | cut -d"-" -f 3)
  
path="$HDFS_DNS_QUERIES/year=$file_year/month=$file_month/day=$file_day"
echo "Anonimize data in: $path"
#see if the dir exists, if not then stop the script
hdfs dfs -test -d "$path" 
if [ $? -ne "0" ]
then
   echo "$path does not exist, stop script"
   exit 0
fi

#move parition to temp table storage
hdfs dfs -mkdir -p "$IMPALA_DNS_TMP_DIR/year=$file_year/month=$file_month/"
hdfs dfs -D dfs.block.size=1073741824 -mv $path "$IMPALA_DNS_TMP_DIR/year=$file_year/month=$file_month/"

#add partition to temp table
for server in $(hdfs dfs -ls $IMPALA_DNS_TMP_DIR/year=$file_year/month=$file_month/day=$file_day/ | grep server= | cut -d= -f5)
do
    echo "Add partition for $server to $IMPALA_TMP_TABLE"
    impala-shell -k -i $IMPALA_NODE -V -q "alter table $IMPALA_TMP_TABLE add partition (year=$file_year,month=$file_month,day=$file_day,server=\"$server\");"
done

#refresh temp table
impala-shell -k -i $IMPALA_NODE -V -q  "refresh $IMPALA_TMP_TABLE;"

#insert data into src table and use null as value for the src column
impala-shell -k -i $IMPALA_NODE -V -q  "insert into $IMPALA_DNS_DWH_TABLE partition(year, month, day, server) select id,unixtime,time,qname,domainname,len,frag,ttl,ipv,
      prot,NULL as src,srcp,dst,dstp,udp_sum,dns_len,aa,
      tc,rd,ra,z,ad,cd,ancount,arcount,nscount,qdcount,
      opcode,rcode,qtype,qclass,country,asn,edns_udp,
      edns_version,edns_do,edns_ping,edns_nsid,edns_dnssec_dau,
      edns_dnssec_dhu,edns_dnssec_n3u,edns_client_subnet,
      edns_other,edns_client_subnet_asn,edns_client_subnet_country,
      labels,res_len,time_micro,resp_frag,proc_time,is_google,is_opendns,
      dns_res_len,year,month,day,server from $IMPALA_TMP_TABLE;"

#remove partition from temp
for server in $(hdfs dfs -ls $IMPALA_DNS_TMP_DIR/year=$file_year/month=$file_month/day=$file_day/ | grep server= | cut -d= -f5)
do
    echo "Drop partition from $server to $IMPALA_TMP_TABLE"
    impala-shell -k -i $IMPALA_NODE -V -q "alter table $IMPALA_TMP_TABLE drop partition (year=$file_year,month=$file_month,day=$file_day,server=\"$server\" );"
done

#remove external parquet files from tmp
hdfs dfs -rm -r -f  "$IMPALA_DNS_TMP_DIR/year*"
#invalidate metadata
impala-shell -k -i $IMPALA_NODE -V -q  "invalidate metadata;"


