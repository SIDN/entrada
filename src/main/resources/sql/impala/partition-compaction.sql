CREATE TABLE ${DATABASE_NAME}.tmp_compaction
WITH (
      external_location = '${TABLE_LOC}',
      format = 'Parquet',
      parquet_compression = 'SNAPPY')
AS SELECT *
FROM ${DATABASE_NAME}.${TABLE_NAME}
WHERE year=${YEAR} AND month=${MONTH} AND day=${DAY} AND server='${SERVER}';