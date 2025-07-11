CREATE TABLE default.source_table (
  id BIGINT,
  name STRING,
  amount DOUBLE
) USING iceberg;

INSERT INTO default.source_table VALUES
  (1, 'Alice', 150.0),
  (2, 'Bob', 50.0),
  (3, 'Carol', 200.0);
