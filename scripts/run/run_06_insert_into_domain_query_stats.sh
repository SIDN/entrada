#!/bin/bash

# This program is not used in the current version of Entrada AWS however it
# should be fully functional.

DAYS_AGO=$1

IMPALA_OPTS=
if [ -f "$KEYTAB_FILE" ];
then
   kinit $KRB_USER -k -t $KEYTAB_FILE
   IMPALA_OPTS=-k
fi


#Target table
TARGET_TABLE="dns.domain_query_stats"

#get date for yesterday
day=$(date --date="$DAYS_AGO days ago" +"%-d")
year=$(date --date="$DAYS_AGO days ago" +"%Y")
month=$(date --date="$DAYS_AGO days ago" +"%-m")

hive -e "
WITH domain_query_counts AS (
select domainname, count(1) as counts,year, month, day
from dns.queries
where year=$year and month=$month and day=$day
group by year, month, day, domainname)
,
domain_src_counts AS (
select domainname, count(distinct(src)) as counts,year, month, day
from dns.queries
where year=$year and month=$month and day=$day
group by year, month, day, domainname)
,
domain_country_counts AS (
select domainname, count(distinct(country)) as counts,year, month, day
from dns.queries
where year=$year and month=$month and day=$day
group by year, month, day, domainname)
,
domain_asn_counts AS (
select domainname, count(distinct(asn)) as counts,year, month, day
from dns.queries
where year=$year and month=$month and day=$day
group by year, month, day, domainname)
insert into $TARGET_TABLE partition(year, month, day)
select q.domainname, q.counts, s.counts, cast(c.counts as int),asn.counts, q.year, q.month, q.day
from domain_query_counts q, domain_src_counts s, domain_country_counts c, domain_asn_counts asn
where q.domainname = s.domainname
and q.domainname = c.domainname
and q.domainname = asn.domainname;


ANALYZE TABLE $TARGET_TABLE partition(year=$year, month=$month, day=$day) COMPUTE STATISTICS;
ANALYZE TABLE $TARGET_TABLE partition(year=$year, month=$month, day=$day) COMPUTE STATISTICS for columns;"
#edit: cant use Impala's COMPUTE STATS, need to use both of the commands above instead
