CREATE TABLE ${DATABASE_NAME}.tmp_compaction
WITH (
      external_location = '${TABLE_LOC}',
      format = 'Parquet',
      parquet_compression = 'SNAPPY')
AS SELECT 
  id, unixtime, time, qname, domainname, len, frag, ttl, ipv,
  prot, src, srcp, dst, dstp, udp_sum, dns_len, aa,
  tc, rd, ra, z, ad, cd, ancount, arcount, nscount, qdcount,
  opcode, rcode, qtype, qclass,
  country, asn, edns_udp, edns_version,
  edns_do, edns_ping, edns_nsid, edns_dnssec_dau,
  edns_dnssec_dhu, edns_dnssec_n3u,
  edns_client_subnet, edns_other,
  edns_client_subnet_asn,
  edns_client_subnet_country,
  labels,res_len,time_micro,resp_frag,proc_time,is_google,is_opendns,
  dns_res_len,server_location,from_unixtime(unixtime) query_ts,
  edns_padding,pcap_file,edns_keytag_count,edns_keytag_list,q_tc,q_ra,q_ad,q_rcode,
  year,month,day,server	   
FROM ${DATABASE_NAME}.${TABLE_NAME}
WHERE year=${YEAR} AND month=${MONTH} AND day=${DAY} AND server='${SERVER}';