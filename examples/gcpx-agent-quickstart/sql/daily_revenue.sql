{{ config(materialized='table') }}
SELECT
  DATE(ordered_at) AS day,
  region,
  SUM({{ cents_to_dollars('amount_cents') }}) AS revenue,
  COUNT(*) AS order_count
FROM {{ source('raw', 'raw_orders') }}
GROUP BY day, region
