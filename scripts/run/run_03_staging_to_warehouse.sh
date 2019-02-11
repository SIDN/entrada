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


#edit: all sections defining impala options and also those working with ICMP have been Removed

#edit: no we dont do that anymore, stop it
# runasImpala(){
#   export HADOOP_USER_NAME=impala
# }


runasSuperuser(){
  export HADOOP_USER_NAME=hdfs
}

#-----------------------------
# Main script
#-----------------------------

#edit: nooooope, also all runas* hereafter will be commented out
#runasImpala
runasSuperuser

####
#### DNS data section
####

#get date in utc
current_date=$(date -u "+%Y%m%d")

#Data is beeing appended to the partition of this day, only process older partitions
for partition in $(hive -e "select year,month,day,server
                    from $IMPALA_DNS_STAGING_TABLE
                    where (concat(cast(year as string),lpad(cast(month as string),2,\"0\"),lpad(cast(day as string),2,\"0\"))) < \"$current_date\"
                    group by year,month,day,server
                    order by year,month,day,server desc;" --output_delimiter=, --quiet --delimited)
do
    year=$(echo $partition | cut -d, -f 1)
    month=$(echo $partition | cut -d, -f 2)
    day=$(echo $partition | cut -d, -f 3)
    server=$(echo $partition | cut -d, -f 4)

    echo "[$(date)] : Move $IMPALA_DNS_STAGING_TABLE table partition year=$year, month=$month, day=$day, server=$server to $IMPALA_DNS_DWH_TABLE"

    #insert all the staging data for yesterday into the datawarehouse table
    #skip the duplicate "svr" column.
    hive -e  "insert into $IMPALA_DNS_DWH_TABLE partition(year, month, day, server) select
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
         dns_res_len,server_location,cast(unixtime as timestamp),
         edns_padding,pcap_file,edns_keytag_count,edns_keytag_list,q_tc,q_ra,q_ad,q_rcode,
         year,month,day,server
         from $IMPALA_DNS_STAGING_TABLE where year=$year and month=$month and day=$day and server=\"$server\";"

    if [ $? -ne 0 ]
    then
        #send mail to indicate error
        echo "[$(date)] : insert data into $IMPALA_DNS_DWH_TABLE failed" | mail -s "Impala error" $ERROR_MAIL
        exit 1
    fi

    #drop partition from the staging table (unlink parquet files)
    echo "[$(date)] : drop $IMPALA_DNS_STAGING_TABLE partition (year=$year,month=$month,day=$day,server=$server)"
    hive -S -e "alter table $IMPALA_DNS_STAGING_TABLE drop partition (year=$year,month=$month,day=$day, server=\"$server\");"

    #delete staging parquet data from hdfs
    # runasSuperuser
    echo "[$(date)] : delete the staging parquet files from hdfs $HDFS_DNS_STAGING/year=$year/month=$month/day=$day/server=$server"
    hdfs dfs -rm -r -f $HDFS_DNS_STAGING/year=$year/month=$month/day=$day/server=$server
    # runasImpala

    #refresh impala metadata for staging table
    echo "[$(date)] : issue refresh for $IMPALA_DNS_STAGING_TABLE"
    hive -S -e "refresh $IMPALA_DNS_STAGING_TABLE;"

    #update the table statistics
    #use the partion spec here, if we don't then Impala we analyze the entire table after adding
    #a new column, this might take very long in case of a large table.
    hive -S -e "COMPUTE INCREMENTAL STATS $IMPALA_DNS_DWH_TABLE PARTITION (year=$year,month=$month,day=$day, server=\"$server\");"

done

echo "[$(date)] : done moving data from staging to queries table"
