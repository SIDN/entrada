-- drop old tables first
drop table if exists entrada_file_archive;

CREATE TABLE IF NOT EXISTS entrada_file_archive
(
  id serial NOT NULL,
  date timestamp NOT NULL,
  file character varying(255) NOT NULL,
  CONSTRAINT entrada_file_archive_pkey PRIMARY KEY (id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_entrada_file_archive_file ON entrada_file_archive (file);


