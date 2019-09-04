ALTER TABLE entrada_file_archive add column if not exists mode character varying(20) NOT NULL DEFAULT 'NONE';

UPDATE entrada_file_archive
SET mode = 'NONE'
WHERE mode IS NULL;