{{
  config(
    materialized='incremental',
    unique_key=['user_id', 'month'],
    partition_by={
      "field": "month",
      "data_type": "date",
      "granularity": "month"
    },
    description='Daily user engagement metrics aggregated monthly. Daily incremental job processes entire current month for fresh data.'
  )
}}
