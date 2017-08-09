use dns;

alter table staging
add columns (edns_padding INT);

alter table queries
add columns (edns_padding INT);

