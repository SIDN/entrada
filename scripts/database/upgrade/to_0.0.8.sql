use dns;

alter table queries
add columns (query_ts TIMESTAMP);