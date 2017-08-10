use dns;

alter table staging
add columns (edns_padding INT, pcap_file STRING, edns_keytag_count INT, edns_keytag_list STRING);

alter table queries
add columns (edns_padding INT, pcap_file STRING, edns_keytag_count INT, edns_keytag_list STRING);

use icmp;

alter table staging
add columns (pcap_file STRING);

alter table queries
add columns (pcap_file STRING);
