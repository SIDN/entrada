export HADOOP_USER_NAME=hdfs

# detect already existing partitions (and thereby data) on these tables
hive -e "MSCK REPAIR TABLE $DNS_STAGING_TABLE; MSCK REPAIR TABLE $DNS_DWH_TABLE; MSCK REPAIR TABLE dns.domain_query_stats;"
# gather table statistics (metadata)
hive -e "
ANALYZE TABLE $DNS_STAGING_TABLE PARTITION(year, month, day, server) COMPUTE STATISTICS;
ANALYZE TABLE $DNS_STAGING_TABLE PARTITION(year, month, day, server) COMPUTE STATISTICS for columns;
ANALYZE TABLE $DNS_DWH_TABLE PARTITION(year, month, day, server) COMPUTE STATISTICS;
ANALYZE TABLE $DNS_DWH_TABLE PARTITION(year, month, day, server) COMPUTE STATISTICS for columns;
ANALYZE TABLE dns.domain_query_stats PARTITION(year, month, day) COMPUTE STATISTICS;
ANALYZE TABLE dns.domain_query_stats PARTITION(year, month, day) COMPUTE STATISTICS for columns;
"
