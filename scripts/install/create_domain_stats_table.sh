#Create the Impala table for the per domain name statistics.

IMPALA_OPTS=
if [ -f "$KEYTAB_FILE" ];
then
   kinit $KRB_USER -k -t $KEYTAB_FILE
   IMPALA_OPTS=-k
fi

#Target table
TARGET_TABLE="domain_query_stats"

impala-shell $IMPALA_OPTS -i $IMPALA_NODE -V -q  "

USE dns;

CREATE TABLE IF NOT EXISTS $TARGET_TABLE (
  domainname STRING,
  qry_count BIGINT,
  uq_src BIGINT,
  uq_country INT,
  uq_asn BIGINT)
  PARTITIONED BY (year INT, month INT, day INT)
  STORED AS PARQUETFILE
  LOCATION '$HDFS_HOME/$TARGET_TABLE';"
