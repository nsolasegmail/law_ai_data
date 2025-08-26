{{
  config(
    materialized='incremental',
    unique_key=['date'],
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    description='Daily analytics dashboard providing key insights and KPIs for Harvey product analytics using dimensional models'
  )
}}

with daily_summary as (
  select 
    date(e.created) as date,
    count(*) as total_events,
    count(distinct e.user_id) as unique_users,
    count(distinct e.firm_id) as unique_firms,
    sum(e.num_docs) as total_documents,
    avg(e.feedback_score) as overall_avg_feedback,
    sum(case when e.event_type = 'ASSISTANT' then 1 else 0 end) as assistant_events,
    sum(case when e.event_type = 'VAULT' then 1 else 0 end) as vault_events,
    sum(case when e.event_type = 'WORKFLOW' then 1 else 0 end) as workflow_events,
    sum(case when e.feedback_score >= 4 then 1 else 0 end) as high_satisfaction_events,
    sum(case when e.feedback_score <= 2 then 1 else 0 end) as low_satisfaction_events
  from {{ ref('events') }} e
  {% if is_incremental() %}
    -- For incremental runs, process only new dates
    where date(e.created) >= coalesce(
      (select max(date) from {{ this }}), 
      '1900-01-01'::date
    )
  {% endif %}
  group by 1
),

daily_firm_engagement as (
  select 
    date,
    count(distinct firm_id) as active_firms,
    sum(total_queries) as total_firm_queries,
    sum(total_documents_processed) as total_firm_documents,
    avg(avg_feedback_score) as avg_firm_feedback,
    avg(daily_adoption_rate) as avg_firm_adoption_rate
  from {{ ref('daily_firm_usage_summary') }}
  {% if is_incremental() %}
    -- For incremental runs, process only new dates
    where date >= coalesce(
      (select max(date) from {{ this }}), 
      '1900-01-01'::date
    )
  {% endif %}
  group by 1
),

daily_cohort_analysis as (
  select 
    date,
    -- New user cohort (users created in this month)
    count(case when months_since_creation = 0 then 1 end) as new_users_cohort,
    -- Returning user cohort (users active in this month who were created before)
    count(case when months_since_creation > 0 then 1 end) as returning_users_cohort,
    -- Power user retention (users who were power users in previous month and still are)
    count(case when power_user_tier = 'Power User' and months_since_creation > 0 then 1 end) as power_user_retention,
    -- User progression (users who moved up tiers)
    count(case when engagement_level in ('High Frequency User', 'Mid Frequency User') and months_since_creation > 0 then 1 end) as user_progression,
    -- Power user counts by tier
    count(case when power_user_tier = 'Power User' then 1 end) as power_users,
    count(case when power_user_tier = 'High Value User' then 1 end) as high_value_users,
    count(case when power_user_tier = 'Medium Value User' then 1 end) as medium_value_users,
    count(case when power_user_tier = 'Low Value User' then 1 end) as low_value_users,
    count(case when power_user_tier = 'Inactive User' then 1 end) as inactive_users,
    -- Average power scores
    avg(power_user_score) as avg_power_score,
    avg(total_queries) as avg_queries_per_user,
    avg(active_days) as avg_active_days_per_user,
    avg(avg_feedback_score) as avg_user_feedback
  from {{ ref('daily_user_engagement') }}
  {% if is_incremental() %}
    -- For incremental runs, process only new dates
    where date >= coalesce(
      (select max(date) from {{ this }}), 
      '1900-01-01'::date
    )
  {% endif %}
  group by 1
),

final_dashboard as (
  select 
    ds.date,
    
    -- Platform Overview
    ds.total_events,
    ds.unique_users,
    ds.unique_firms,
    ds.total_documents,
    ds.overall_avg_feedback,
    
    -- User Engagement & Power Users
    dca.power_users,
    dca.high_value_users,
    dca.medium_value_users,
    dca.low_value_users,
    dca.inactive_users,
    dca.avg_power_score,
    dca.avg_queries_per_user,
    dca.avg_active_days_per_user,
    dca.avg_user_feedback,
    
    -- Firm Engagement
    dfe.active_firms,
    dfe.total_firm_queries,
    dfe.total_firm_documents,
    dfe.avg_firm_feedback,
    dfe.avg_firm_adoption_rate,
    
    -- Event Distribution
    ds.assistant_events,
    ds.vault_events,
    ds.workflow_events,
    ds.high_satisfaction_events,
    ds.low_satisfaction_events,
    
    -- Percentages
    case when ds.total_events > 0 then round((ds.assistant_events::float / ds.total_events * 100), 2) else 0 end as assistant_percent,
    case when ds.total_events > 0 then round((ds.vault_events::float / ds.total_events * 100), 2) else 0 end as vault_percent,
    case when ds.total_events > 0 then round((ds.workflow_events::float / ds.total_events * 100), 2) else 0 end as workflow_percent,
    case when ds.total_events > 0 then round((ds.high_satisfaction_events::float / ds.total_events * 100), 2) else 0 end as high_satisfaction_rate,
    case when ds.total_events > 0 then round((ds.low_satisfaction_events::float / ds.total_events * 100), 2) else 0 end as low_satisfaction_rate,
    
    -- Efficiency Metrics
    case when ds.unique_users > 0 then round((ds.total_events::float / ds.unique_users), 2) else 0 end as events_per_user,
    case when ds.unique_users > 0 then round((ds.total_documents::float / ds.unique_users), 2) else 0 end as documents_per_user,
    case when ds.total_events > 0 then round((ds.total_documents::float / ds.total_events), 2) else 0 end as avg_documents_per_event,
    
    -- Cohort Analysis
    dca.new_users_cohort,
    dca.returning_users_cohort,
    dca.power_user_retention,
    dca.user_progression,
    
    -- Power User Rates
    case when ds.unique_users > 0 then round((dca.power_users::float / ds.unique_users * 100), 2) else 0 end as power_users_rate,
    case when ds.unique_users > 0 then round(((dca.power_users + dca.high_value_users)::float / ds.unique_users * 100), 2) else 0 end as high_value_users_rate,
    
    -- Usage Volume Category
    case 
      when ds.total_events >= 10000 then 'High Volume'
      when ds.total_events >= 5000 then 'Medium Volume'
      when ds.total_events >= 1000 then 'Low Volume'
      else 'Minimal Usage'
    end as usage_volume_category,
    
    -- ETL metadata
    current_timestamp as _loaded_at,
    '{{ invocation_id }}' as _run_id
    
  from daily_summary ds
  left join daily_firm_engagement dfe on ds.date = dfe.date
  left join daily_cohort_analysis dca on ds.date = dca.date
)

select * from final_dashboard
order by date desc

{% if is_incremental() %}
  -- Incremental logic: only process new dates
  where date >= coalesce(
    (select max(date) from {{ this }}), 
    '1900-01-01'::date
  )
{% endif %}
