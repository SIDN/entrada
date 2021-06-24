/*
 * SQL-query for purging PII attributes from parquet data files.
 * Create a temporary external table and insert the partition data into this table.
 * Using the NOSHUFFLE query hint to prevent Impala from adding a network
 * shuffle operation to the query plan.
 * Shuffling data across the network will make the query run much slower
 * see: https://impala.apache.org/docs/build/html/topics/impala_hints.html
 */
CREATE /* +NOSHUFFLE */ EXTERNAL TABLE ${DATABASE_NAME}.tmp_compaction
  PARTITIONED BY (year, month, day, server)
  COMMENT 'ENTRADA Compaction temp table'
  STORED AS PARQUET
  LOCATION '${TABLE_LOC}'
AS SELECT 
	id,
	time,
	qname,
	domainname,
	frag,
	ttl,
	ipv,
  	prot,
 	srcp,
 	dst,
 	dstp,
 	aa,
 	tc,
 	rd,
 	ra,
 	z,
 	ad,
 	cd,
 	ancount,
 	arcount,
 	nscount,
 	qdcount,
 	opcode,
 	rcode,
 	qtype,
 	qclass,
  	country,
  	asn,
  	asn_organisation,
  	edns_udp,
  	edns_version,
  	edns_do,
  	edns_ping,
  	edns_nsid, 
  	edns_dnssec_dau,
  	edns_dnssec_dhu,
  	edns_dnssec_n3u,
  	edns_other,
  	edns_client_subnet_asn,
  	edns_client_subnet_asn_organisation,
  	edns_client_subnet_country,
  	labels,
  	resp_frag,
  	proc_time,
  	server_location,
  	edns_padding,
  	pcap_file,
  	edns_keytag_count,
  	edns_keytag_list,
  	q_tc,
  	q_ra,
  	q_ad,
  	pub_resolver,
  	req_len,
  	res_len,
  	tcp_hs_rtt,
    query_ts,
    req_ip_df,
    res_ip_df,
  	year,
  	month,
  	day,
  	server	   
FROM ${DATABASE_NAME}.${TABLE_NAME}
WHERE year=${YEAR} AND month=${MONTH} AND day=${DAY} AND server='${SERVER}';
