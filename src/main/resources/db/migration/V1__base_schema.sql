-- drop old tables first
drop table if exists entrada_file_archive;

CREATE TABLE IF NOT EXISTS entrada_file_archive
(
  id serial NOT NULL,
  date_start timestamp NOT NULL,
  date_end timestamp NOT NULL,
  time int NOT NULL,
  file character varying(255) NOT NULL,
  server character varying(255) NOT NULL,
  path character varying(255) NOT NULL,
  rows int NOT NULL,
  CONSTRAINT entrada_file_archive_pkey PRIMARY KEY (id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_entrada_file_archive_file_server ON entrada_file_archive (file,server);


CREATE TABLE IF NOT EXISTS entrada_partition
(
  id serial NOT NULL,
  table_name character varying(255) NOT NULL,
  created timestamp NOT NULL,
  year int NOT NULL,
  month int NOT NULL,
  day int NOT NULL,
  server character varying(255) NOT NULL,
  path character varying(255) NOT NULL,
  compaction_ts timestamp,
  compaction_time int,
  updated_ts timestamp,
  CONSTRAINT entrada_partition_pkey PRIMARY KEY (id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_entrada_partition_path ON entrada_partition (table_name,year,month,day,server);