ALTER TABLE entrada_partition add column if not exists privacy_purge_ts timestamp;
ALTER TABLE entrada_partition add column if not exists privacy_purge_ok boolean;
ALTER TABLE entrada_partition add column if not exists privacy_purge_time int;
