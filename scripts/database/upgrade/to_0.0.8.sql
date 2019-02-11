use dns;

alter table queries
add columns (query_ts TIMESTAMP);

use icmp;

alter table packets
add columns (query_ts TIMESTAMP);