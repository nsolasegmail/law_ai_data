{{
  config(
    materialized='incremental',
    unique_key=['firm_id', 'date'],
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    description='Daily firm usage summary with point-in-time accurate firm attributes'
  )
}}

with daily_firm_events as (
  select 
    e.firm_id,
    e.user_id,
    e.event_type,
    e.num_docs,
    e.feedback_score,
    e.created as event_date,
    date(e.created) as date,
    -- Point-in-time accurate firm attributes
    df.firm_size,
    df.arr_in_thousands,
    df.firm_created_date,
    df.effective_start_date,
    df.effective_end_date,
    df.is_current
  from {{ ref('events') }} e
  -- Point-in-time join with dim_firms for accurate firm attributes at event time
  left join {{ ref('dim_firms') }} df on e.firm_id = df.firm_id
    and e.created >= df.effective_start_date 
    and e.created <= df.effective_end_date
  {% if is_incremental() %}
    -- For incremental runs, process only new dates
    where date(e.created) >= coalesce(
      (select max(date) from {{ this }}), 
      '1900-01-01'::date
    )
  {% endif %}
),

daily_firm_metrics as (
  select 
    firm_id,
    date,
    firm_size,
    arr_in_thousands,
    firm_created_date,
    -- User counts
    count(distinct user_id) as active_users,
    -- Event metrics
    count(*) as total_queries,
    sum(num_docs) as total_documents_processed,
    avg(feedback_score) as avg_feedback_score,
    -- Event type breakdown
    sum(case when event_type = 'ASSISTANT' then 1 else 0 end) as assistant_queries,
    sum(case when event_type = 'VAULT' then 1 else 0 end) as vault_queries,
    sum(case when event_type = 'WORKFLOW' then 1 else 0 end) as workflow_queries,
    -- Efficiency metrics
    avg(num_docs) as avg_documents_per_query,
    -- User engagement
    count(distinct user_id) as unique_users,
    -- Date calculations
    date_diff('day', firm_created_date, date) as days_since_firm_creation
  from daily_firm_events
  group by 1, 2, 3, 4, 5
),

firm_classifications as (
  select 
    *,
    -- Firm health classification using macro
    {{ classify_firm_health('avg_feedback_score', 'total_queries::float / nullif(active_users, 0)') }} as firm_health_status,
    
    -- Firm maturity stage
    case 
      when days_since_firm_creation <= 30 then 'New Firm'
      when days_since_firm_creation <= 90 then 'Early Stage'
      when days_since_firm_creation <= 180 then 'Growing'
      when days_since_firm_creation <= 365 then 'Established'
      else 'Mature'
    end as firm_maturity_stage,
    
    -- Firm size category
    case 
      when firm_size >= 500 then 'Large Firm'
      when firm_size >= 100 then 'Medium Firm'
      when firm_size >= 25 then 'Small Firm'
      else 'Boutique Firm'
    end as firm_size_category,
    
    -- ARR tier
    case 
      when arr_in_thousands >= 1000 then 'High Value'
      when arr_in_thousands >= 500 then 'Medium Value'
      when arr_in_thousands >= 100 then 'Standard Value'
      else 'Entry Level'
    end as arr_tier,
    
    -- Usage pattern
    {{ classify_usage_pattern('assistant_queries', 'vault_queries', 'workflow_queries') }} as firm_usage_pattern,
    
    -- Daily adoption rate
    case 
      when firm_size > 0 then round((active_users::float / firm_size * 100), 2)
      else 0
    end as daily_adoption_rate,
    
    -- Adoption level classification
    case 
      when (active_users::float / nullif(firm_size, 0)) >= 0.8 then 'High Adoption'
      when (active_users::float / nullif(firm_size, 0)) >= 0.6 then 'Good Adoption'
      when (active_users::float / nullif(firm_size, 0)) >= 0.4 then 'Moderate Adoption'
      when (active_users::float / nullif(firm_size, 0)) >= 0.2 then 'Low Adoption'
      else 'Very Low Adoption'
    end as adoption_level
    
  from daily_firm_metrics
),

final_daily_metrics as (
  select 
    firm_id,
    date,
    firm_size,
    arr_in_thousands,
    firm_created_date,
    days_since_firm_creation,
    
    -- User metrics
    active_users,
    unique_users,
    
    -- Activity metrics
    total_queries,
    total_documents_processed,
    avg_feedback_score,
    
    -- Event type breakdown
    assistant_queries,
    vault_queries,
    workflow_queries,
    
    -- Efficiency metrics
    avg_documents_per_query,
    
    -- Classifications
    firm_health_status,
    firm_maturity_stage,
    firm_size_category,
    arr_tier,
    firm_usage_pattern,
    daily_adoption_rate,
    adoption_level,
    
    -- ETL metadata
    current_timestamp as _loaded_at,
    '{{ invocation_id }}' as _run_id,
    'daily_incremental' as _etl_strategy
    
  from firm_classifications
)

select * from final_daily_metrics
order by firm_id, date

{% if is_incremental() %}
  -- Incremental logic: only process new dates
  where date >= coalesce(
    (select max(date) from {{ this }}), 
    '1900-01-01'::date
  )
{% endif %}
