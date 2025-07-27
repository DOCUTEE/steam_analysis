{% set batch_start = var("batch_start", "2010-10-15 00:00:00") %}
{% set batch_start_ts = "timestamp('" ~ batch_start ~ "')" %}
{% set batch_end_ts = batch_start_ts ~ " + interval 1 day - interval 1 second" %}

{{ config(
    unique_key='time_key',
    partition_by=['year', 'month']
) }}


WITH time_range AS (
  SELECT sequence(
    {{ batch_start_ts }},
    {{ batch_end_ts }},
      interval 1 second
  ) AS ts_array
),
exploded AS (
  SELECT explode(ts_array) AS time_key
  FROM time_range
)

SELECT
  unix_timestamp(time_key) AS time_key,
  YEAR(time_key) AS year,
  QUARTER(time_key) AS quarter,
  MONTH(time_key) AS month,
  WEEKOFYEAR(time_key) AS week,
  DAY(time_key) AS day,
  DAYOFWEEK(time_key) AS day_of_week,
  DATE_FORMAT(time_key, 'EEEE') AS day_name,
  DATE_FORMAT(time_key, 'MMMM') AS month_name,
  CASE WHEN DAYOFWEEK(time_key) IN (1, 7) THEN true ELSE false END AS is_weekend,
  HOUR(time_key) AS hour,
  MINUTE(time_key) AS minute,
  SECOND(time_key) AS second
FROM exploded