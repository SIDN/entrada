use dns;

alter table staging
add columns (edns_padding INT, pcap_file STRING, edns_keytag_count INT, edns_keytag_list STRING, q_tc BOOLEAN, q_ra BOOLEAN, q_ad BOOLEAN, q_rcode INT );

alter table queries
add columns (edns_padding INT, pcap_file STRING, edns_keytag_count INT, edns_keytag_list STRING, q_tc BOOLEAN, q_ra BOOLEAN, q_ad BOOLEAN, q_rcode INT);

use icmp;

alter table staging
add columns (pcap_file STRING);

alter table queries
add columns (pcap_file STRING);
