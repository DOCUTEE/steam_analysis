{{ 
  config(
    schema='silver',
    materialized='table',
    file_format='iceberg'
  ) 
}}

SELECT *
FROM VALUES
  (1, 'Alice', 150.0),
  (2, 'Bob', 50.0),
  (3, 'Carol', 200.0)
AS t(id, name, amount);
