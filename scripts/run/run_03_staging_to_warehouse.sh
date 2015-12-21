#!/usr/bin/env bash

HDFS_DNS_STAGING="$HDFS_HOME/staging"
HDFS_ICMP_STAGING="$HDFS_HOME/icmp-staging"
TMP_FILE=$TMP_DIR/staging-partitions.tmp.csv

IMPALA_DNS_STAGING_TABLE="dns.staging"
IMPALA_ICMP_STAGING_TABLE="icmp.staging"
IMPALA_DNS_DWH_TABLE="dns.queries"
IMPALA_ICMP_DWH_TABLE="icmp.packets"

#use kerberos user "hdfs"
if [ -f $KEYTAB_FILE ];
then
   kinit $KRB_USER -k -t $KEYTAB_FILE
fi

#run all hdfs action as user impala
export HADOOP_USER_NAME=impala

#-----------------------------
# Main script
#-----------------------------

  #get date for yesterday
  day=$(date --date="1 days ago" +"%d")
  year=$(date --date="1 days ago" +"%Y")
  month=$(date --date="1 days ago" +"%m")
  echo "yesterday --> day:$day month:$month year:$year"


   ####
   #### DNS data section
   ####

  #insert all the staging data for yesterday into the datawarehouse table
  #skip the duplicate "svr" column.
  impala-shell -k -i $IMPALA_NODE -V -q  "insert into $IMPALA_DNS_DWH_TABLE partition(year, month, day, server) select 
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
     dns_res_len,year,month,day,server
     from $IMPALA_DNS_STAGING_TABLE where year=$year and month=$month and day=$day;"

   echo "Issue refresh for $IMPALA_DNS_DWH_TABLE"
   impala-shell -k --quiet -i $IMPALA_NODE -V -q "refresh $IMPALA_DNS_DWH_TABLE;"
   if [ $? -ne 0 ]
   then
     #send mail to indicate error
     echo "Refresh metadata $IMPALA_DNS_DWH_TABLE failed" | mail -s "Impala error" $ERROR_MAIL
     exit 1
   fi

   #delete partition from staging
   impala-shell --print_header -q "show partitions $IMPALA_DNS_STAGING_TABLE" -B -i $IMPALA_NODE --output_delimiter=, -o $TMP_FILE
   grep ,$day, $TMP_FILE | cut -d, -f4 | while read -r server ; do
      echo "Drop staging partition for $server"
      impala-shell -c -k --quiet -i $IMPALA_NODE -V -q "alter table $IMPALA_DNS_STAGING_TABLE drop partition (year=$year,month=$month,day=$day, server=\"$server\");"
   done

   #refresh impala metadata
   echo "Issue refresh for $IMPALA_DNS_STAGING_TABLE"
   impala-shell -k --quiet -i $IMPALA_NODE -V -q "refresh $IMPALA_DNS_STAGING_TABLE;"
   if [ $? -ne 0 ]
   then
     #send mail to indicate error
     echo "Refresh metadata $IMPALA_DNS_STAGING_TABLE failed" | mail -s "Impala error" $ERROR_MAIL
     exit 1
   fi

   #get new rows count
   rows=$(impala-shell -k --quiet -i $IMPALA_NODE -q "select count(1) from $IMPALA_DNS_DWH_TABLE where year=$year and month=$month and day=$day;" --quiet -B)
   echo "added $rows new rows to $IMPALA_DNS_DWH_TABLE"
   
   #delete staging data from hdfs
   echo "delete the staging parquet files from hdfs $HDFS_DNS_STAGING/year\=$year/month\=$month/day\=$day"
   hdfs dfs -rm -r -f $HDFS_DNS_STAGING/year\=$year/month\=$month/day\=$day

   #update the table statistics
   impala-shell -c -k --quiet -i $IMPALA_NODE -q "COMPUTE INCREMENTAL STATS $IMPALA_DNS_DWH_TABLE;" 
 
   ####
   #### ICMP data section
   ####

   #Get month and day without leading zero
   day=$(date --date="1 days ago" +"%-d")
   month=$(date --date="1 days ago" +"%-m")

   #skip the duplicate "svr" column.
  impala-shell -k -i $IMPALA_NODE -V -q  "insert into $IMPALA_ICMP_DWH_TABLE partition(year, month, day) select 
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
      orig_dns_labels,svr,time_micro,year,month,day 
     from $IMPALA_ICMP_STAGING_TABLE where year=$year and month=$month and day=$day;"

   echo "Issue refresh for $IMPALA_ICMP_DWH_TABLE"
   impala-shell -k --quiet -i $IMPALA_NODE -V -q "refresh $IMPALA_ICMP_DWH_TABLE;"
   if [ $? -ne 0 ]
   then
     #send mail to indicate error
     echo "Refresh metadata $IMPALA_ICMP_DWH_TABLE failed" | mail -s "Impala error" $ERROR_MAIL
     exit 1
   fi


   #delete partition from staging
   impala-shell -c -k --quiet -i $IMPALA_NODE -V -q "alter table $IMPALA_ICMP_STAGING_TABLE drop partition (year=$year,month=$month,day=$day);"
   #when adding more servers, don't forget to drop partitions

   #refresh impala metadata
   echo "Issue refresh for $IMPALA_ICMP_STAGING_TABLE"
   impala-shell -k --quiet -i $IMPALA_NODE -V -q "refresh $IMPALA_ICMP_STAGING_TABLE;"
   if [ $? -ne 0 ]
   then
     #send mail to indicate error
     echo "Refresh metadata $IMPALA_ICMP_STAGING_TABLE failed" | mail -s "Impala error" $ERROR_MAIL
     exit 1
   fi

   #get new rows count
   rows=$(impala-shell -k --quiet -i $IMPALA_NODE -q "select count(1) from $IMPALA_ICMP_DWH_TABLE where year=$year and month=$month and day=$day;" --quiet -B)
   echo "added $rows new rows to $IMPALA_ICMP_DWH_TABLE"
   
   #delete staging data from hdfs
   echo "delete the staging parquet files from hdfs $HDFS_ICMP_STAGING/year\=$year/month\=$month/day\=$day"
   hdfs dfs -rm -r -f $HDFS_ICMP_STAGING/year\=$year/month\=$month/day\=$day

   #update the table statistics
   impala-shell -k --quiet -i $IMPALA_NODE -q "COMPUTE INCREMENTAL STATS $IMPALA_ICMP_DWH_TABLE;" 
   
echo "Done"


