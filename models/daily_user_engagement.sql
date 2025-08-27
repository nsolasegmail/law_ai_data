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

with clean_events as (
  select 
    e.user_id,
    e.firm_id,
    e.event_type,
    e.num_docs,
    -- Handle invalid feedback scores by setting them to NULL
    case 
      when e.feedback_score >= 1 and e.feedback_score <= 5 then e.feedback_score
      else null
    end as feedback_score,
    e.created as event_date,
    {{ get_month_start('e.created') }} as month
  from {{ ref('events') }} e
  where 
    -- Ignore records with NULL user_id
    e.user_id is not null
    -- Ignore records with future dates (created > execution date)
    and e.created <= current_date
    {% if is_incremental() %}
      -- For incremental runs, process the entire current month
      -- This allows daily updates with fresh data for the current month
      and {{ get_month_start('e.created') }} = {{ get_month_start('{{ ds }}') }}
    {% endif %}
),

user_events as (
  select
    e.user_id, e.firm_id, e.event_type, e.num_docs, e.feedback_score,
    e.event_date,
    e.month,
    -- Point-in-time accurate user attributes
    du.user_role,
    du.user_created_date,
    du.effective_start_date,
    du.effective_end_date,
    du.is_current
  from clean_events e
  -- Point-in-time join with dim_user for accurate user attributes at event time
  left join {{ ref('dim_user') }} du on e.user_id = du.user_id
    and e.event_date >= du.effective_start_date
    and e.event_date <= du.effective_end_date
),

monthly_user_metrics as (
  select
    user_id,
    firm_id,
    month,
    user_role,
    user_created_date,
    effective_start_date,
    effective_end_date,
    is_current,
    
    -- Basic metrics
    count(*) as total_queries,
    count(distinct event_type) as unique_event_types,
    sum(num_docs) as total_documents_processed,
    avg(feedback_score) as avg_feedback_score,
    count(distinct date(event_date)) as active_days,
    
    -- Date ranges
    min(event_date) as first_activity_date,
    max(event_date) as last_activity_date,
    
    -- Event type breakdown
    sum(case when event_type = 'ASSISTANT' then 1 else 0 end) as assistant_queries,
    sum(case when event_type = 'VAULT' then 1 else 0 end) as vault_queries,
    sum(case when event_type = 'WORKFLOW' then 1 else 0 end) as workflow_queries,
    
    -- Calculated metrics
    case 
      when count(*) > 0 then round(sum(num_docs)::float / count(*), 2)
      else 0
    end as avg_documents_per_query,
    
    -- Months since user creation
    case 
      when user_created_date is not null then 
        date_diff('month', user_created_date, month)
      else null
    end as months_since_creation
    
  from user_events
  group by 1, 2, 3, 4, 5, 6, 7, 8
),

engagement_scoring as (
  select
    *,
    -- Engagement level classification
    {{ classify_engagement_level('total_queries', 'active_days') }} as engagement_level,
    
    -- Usage pattern classification
    {{ classify_usage_pattern('assistant_queries', 'vault_queries', 'workflow_queries') }} as usage_pattern,
    
    -- Quality level classification
    {{ classify_quality_level('avg_feedback_score') }} as quality_level
  from monthly_user_metrics
),

power_user_scoring as (
  select
    *,
    -- Power User Score Components
    -- Active Days Weight (0-100 scale)
    case
      when active_days >= 20 then 100
      when active_days >= 15 then 80
      when active_days >= 10 then 60
      when active_days >= 5 then 40
      when active_days >= 2 then 20
      else 0
    end as active_days_weight,

    -- Document Volume Weight (0-100 scale)
    case
      when total_documents_processed >= 100 then 100
      when total_documents_processed >= 50 then 80
      when total_documents_processed >= 25 then 60
      when total_documents_processed >= 10 then 40
      when total_documents_processed >= 5 then 20
      else 0
    end as document_volume_weight,

    -- Feature Breadth Weight (0-100 scale)
    case
      when unique_event_types = 3 then 100
      when unique_event_types = 2 then 70
      when unique_event_types = 1 then 40
      else 0
    end as feature_breadth_weight

  from engagement_scoring
),

final_metrics as (
  select
    user_id, firm_id, month, user_role, user_created_date, months_since_creation,
    total_queries, unique_event_types, total_documents_processed, avg_feedback_score, active_days,
    first_activity_date, last_activity_date, assistant_queries, vault_queries, workflow_queries,
    avg_documents_per_query, engagement_level, usage_pattern, quality_level,

    -- Power User Score Components
    active_days_weight,
    document_volume_weight,
    feature_breadth_weight,

    -- Power User Score (0-100 scale)
    round(
      0.4 * active_days_weight +
      0.3 * document_volume_weight +
      0.3 * feature_breadth_weight, 2
    ) as power_user_score,

    -- Power User Classification
    case
      when (0.4 * active_days_weight + 0.3 * document_volume_weight + 0.3 * feature_breadth_weight) >= 80 then 'Power User'
      when (0.4 * active_days_weight + 0.3 * document_volume_weight + 0.3 * feature_breadth_weight) >= 60 then 'High Value User'
      when (0.4 * active_days_weight + 0.3 * document_volume_weight + 0.3 * feature_breadth_weight) >= 40 then 'Medium Value User'
      when (0.4 * active_days_weight + 0.3 * document_volume_weight + 0.3 * feature_breadth_weight) >= 20 then 'Low Value User'
      else 'Inactive User'
    end as power_user_tier,

    -- Tenure band instead of lifecycle stage
    case
      when months_since_creation <= 1 then '0-1 months'
      when months_since_creation <= 3 then '1-3 months'
      when months_since_creation <= 6 then '3-6 months'
      when months_since_creation <= 12 then '6-12 months'
      else '12+ months'
    end as tenure_band,

    -- ETL metadata for tracking
    current_timestamp as _loaded_at,
    '{{ invocation_id }}' as _run_id,
    'daily_incremental' as _etl_strategy,
    case
      when month = {{ get_month_start('{{ ds }}') }} then 'current_month'
      else 'historical_month'
    end as _data_status

  from power_user_scoring
)

select * from final_metrics
order by user_id, month
{% if is_incremental() %}
  -- Incremental logic: process entire current month
  -- This overwrites the current month partition daily with fresh data
  where month = {{ get_month_start('{{ ds }}') }}
{% endif %}
