# 2020-10-04: add column for IP MTU from ICMP msg "Packet Too Big"
ALTER TABLE ${DATABASE_NAME}.${TABLE_NAME} ADD COLUMNS ( icmp_ip_mtu INT )
ALTER TABLE ${DATABASE_NAME}.${TABLE_NAME} ADD COLUMNS ( ip_asn_organisation STRING )