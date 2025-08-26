{{
  config(
    materialized='incremental',
    unique_key=['user_dim_key'],
    description='Dimension table for users with SCD Type 2 tracking for point-in-time accuracy - updated daily'
  )
}}

with user_changes as (
  select 
    id as user_id,
    title as user_role,
    created as user_created_date,
    -- Track changes in user attributes
    row_number() over (
      partition by id 
      order by created
    ) as change_number,
    -- Create effective dates
    created as effective_start_date,
    lead(created) over (
      partition by id 
      order by created
    ) as effective_end_date
  from {{ ref('users') }}
  {% if is_incremental() %}
    -- For incremental runs, process only new or changed users
    where created >= coalesce(
      (select max(effective_start_date) from {{ this }}), 
      '1900-01-01'::date
    )
  {% endif %}
),

final_dim_user as (
  select 
    user_id,
    user_role,
    user_created_date,
    effective_start_date,
    -- Set end_date to '9999-12-31' for current records, otherwise use next effective date
    case 
      when effective_end_date is null then '9999-12-31'::date
      else effective_end_date - interval '1 day'
    end as effective_end_date,
    -- Current flag: true if this is the most recent record for the user
    case 
      when effective_end_date is null then true
      else false
    end as is_current,
    -- Surrogate key for the dimension record
    concat(user_id, '_', change_number) as user_dim_key,
    -- Metadata
    current_timestamp as _loaded_at,
    '{{ invocation_id }}' as _run_id
  from user_changes
)

select * from final_dim_user
order by user_id, effective_start_date

{% if is_incremental() %}
  -- Incremental logic: only process new or changed users
  where effective_start_date >= coalesce(
    (select max(effective_start_date) from {{ this }}), 
    '1900-01-01'::date
  )
{% endif %}
