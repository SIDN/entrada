# 2020-10-04: add column for IP response packet DF flag
ALTER TABLE ${DATABASE_NAME}.${TABLE_NAME} ADD COLUMNS ( req_ip_df boolean )
ALTER TABLE ${DATABASE_NAME}.${TABLE_NAME} ADD COLUMNS ( res_ip_df boolean )