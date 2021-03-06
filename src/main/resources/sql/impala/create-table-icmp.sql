create external table if not exists ${DATABASE_NAME}.${TABLE_NAME} (
	time bigint,
	icmp_type SMALLINT,
	icmp_code SMALLINT,
	icmp_echo_client_type SMALLINT,
	icmp_ip_mtu INT,
	ip_ttl SMALLINT,
	ip_v TINYINT,
	ip_src STRING,
	ip_dst STRING,
	ip_country STRING,
	ip_asn STRING,
	ip_asn_organisation STRING,
	ip_len INT,
	l4_prot INT,
	l4_srcp INT,
	l4_dstp INT,
	orig_ip_ttl SMALLINT,
	orig_ip_v SMALLINT,
	orig_ip_src STRING,
	orig_ip_dst STRING,
	orig_l4_prot INT,
	orig_l4_srcp INT,
	orig_l4_dstp INT,
	orig_ip_len INT,
	orig_icmp_type smallint,
	orig_icmp_code smallint,
	orig_icmp_echo_client_type smallint,
	orig_dns_id INT,
	orig_dns_qname STRING,
	orig_dns_domainname STRING,
	orig_dns_len INT,
	orig_dns_aa BOOLEAN,
	orig_dns_tc BOOLEAN,
	orig_dns_rd BOOLEAN,
	orig_dns_ra BOOLEAN,
	orig_dns_z BOOLEAN,
	orig_dns_ad BOOLEAN,
	orig_dns_cd BOOLEAN,
	orig_dns_ancount INT,
	orig_dns_arcount INT,
	orig_dns_nscount INT,
	orig_dns_qdcount INT,
	orig_dns_rcode INT,
	orig_dns_qtype INT,
	orig_dns_opcode INT,
	orig_dns_qclass INT,
	orig_dns_edns_udp INT,
	orig_dns_edns_version SMALLINT,
	orig_dns_edns_do BOOLEAN,
	orig_dns_labels INT,
	server_location STRING,
	pcap_file STRING,
	ip_pub_resolver STRING,
	query_ts TIMESTAMP,
	server STRING
) partitioned by (year INT, month INT, day INT)
  STORED AS PARQUETFILE
  LOCATION '${TABLE_LOC}';

 
