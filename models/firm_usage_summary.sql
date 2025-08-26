{{
  config(
    materialized='table',
    description='Firm-level aggregate metrics combining firm metadata with usage and user metrics for firm health assessment'
  )
}}

with firm_events as (
  select 
    e.firm_id,
    e.user_id,
    e.event_type,
    e.num_docs,
    e.feedback_score,
    e.created as event_date,
    {{ get_month_start('e.created') }} as month,
    f.firm_size,
    f.arr_in_thousands,
    f.created as firm_created_date
  from {{ ref('events') }} e
  left join {{ ref('firms') }} f on e.firm_id = f.id
),

firm_monthly_metrics as (
  select 
    firm_id,
    month,
    firm_size,
    arr_in_thousands,
    firm_created_date,
    -- User metrics
    count(distinct user_id) as active_users,
    count(distinct case when month = {{ get_month_start('current_date') }} then user_id end) as current_month_active_users,
    -- Usage metrics
    count(*) as total_queries,
    sum(num_docs) as total_documents_processed,
    avg(feedback_score) as avg_feedback_score,
    -- Event type breakdown
    sum(case when event_type = 'ASSISTANT' then 1 else 0 end) as assistant_queries,
    sum(case when event_type = 'VAULT' then 1 else 0 end) as vault_queries,
    sum(case when event_type = 'WORKFLOW' then 1 else 0 end) as workflow_queries,
    -- Efficiency metrics
    avg(num_docs) as avg_documents_per_query,
    -- Firm tenure
    {{ get_months_between('firm_created_date', 'month') }} as months_since_firm_creation
  from firm_events
  group by 1, 2, 3, 4, 5
),

firm_user_roles as (
  select 
    e.firm_id,
    u.title as user_role,
    count(distinct u.id) as user_count
  from {{ ref('events') }} e
  left join {{ ref('users') }} u on e.user_id = u.id
  group by 1, 2
),

firm_role_summary as (
  select 
    firm_id,
    sum(case when user_role = 'Partner' then user_count else 0 end) as partner_count,
    sum(case when user_role = 'Senior Associate' then user_count else 0 end) as senior_associate_count,
    sum(case when user_role = 'Associate' then user_count else 0 end) as associate_count,
    sum(case when user_role = 'Junior Associate' then user_count else 0 end) as junior_associate_count,
    sum(user_count) as total_users
  from firm_user_roles
  group by 1
),

firm_health_scoring as (
  select 
    fm.*,
    fr.partner_count,
    fr.senior_associate_count,
    fr.associate_count,
    fr.junior_associate_count,
    fr.total_users,
    
    -- Usage per user metrics
    case 
      when fr.total_users > 0 then fm.total_queries / fr.total_users 
      else 0 
    end as queries_per_user,
    
    case 
      when fr.total_users > 0 then fm.total_documents_processed / fr.total_users 
      else 0 
    end as documents_per_user,
    
    -- Firm health score using macro
    {{ classify_firm_health('fm.avg_feedback_score', 'queries_per_user') }} as firm_health_status,
    
    -- Firm maturity classification
    case 
      when fm.months_since_firm_creation <= 3 then 'New Firm'
      when fm.months_since_firm_creation <= 6 then 'Early Stage'
      when fm.months_since_firm_creation <= 12 then 'Growing'
      when fm.months_since_firm_creation <= 24 then 'Established'
      else 'Mature'
    end as firm_maturity_stage,
    
    -- Size-based classification
    case 
      when fm.firm_size >= 200 then 'Large Firm'
      when fm.firm_size >= 100 then 'Medium Firm'
      when fm.firm_size >= 50 then 'Small Firm'
      else 'Boutique Firm'
    end as firm_size_category,
    
    -- ARR-based classification
    case 
      when fm.arr_in_thousands >= 300 then 'High Value'
      when fm.arr_in_thousands >= 150 then 'Medium Value'
      when fm.arr_in_thousands >= 75 then 'Standard Value'
      else 'Entry Level'
    end as arr_tier,
    
    -- Usage pattern classification using macro
    {{ classify_usage_pattern('fm.assistant_queries', 'fm.vault_queries', 'fm.workflow_queries') }} as firm_usage_pattern,
    
    -- Adoption rate
    case 
      when fr.total_users > 0 then 
        round((fm.active_users::float / fr.total_users::float) * 100, 2)
      else 0 
    end as user_adoption_rate
    
  from firm_monthly_metrics fm
  left join firm_role_summary fr on fm.firm_id = fr.firm_id
)

select 
  firm_id,
  month,
  firm_size,
  arr_in_thousands,
  firm_created_date,
  months_since_firm_creation,
  
  -- User composition
  total_users,
  active_users,
  current_month_active_users,
  partner_count,
  senior_associate_count,
  associate_count,
  junior_associate_count,
  
  -- Usage metrics
  total_queries,
  total_documents_processed,
  avg_feedback_score,
  assistant_queries,
  vault_queries,
  workflow_queries,
  
  -- Efficiency metrics
  avg_documents_per_query,
  queries_per_user,
  documents_per_user,
  
  -- Classifications
  firm_health_status,
  firm_maturity_stage,
  firm_size_category,
  arr_tier,
  firm_usage_pattern,
  user_adoption_rate,
  
  -- Additional insights
  case 
    when user_adoption_rate >= 80 then 'High Adoption'
    when user_adoption_rate >= 60 then 'Good Adoption'
    when user_adoption_rate >= 40 then 'Moderate Adoption'
    when user_adoption_rate >= 20 then 'Low Adoption'
    else 'Very Low Adoption'
  end as adoption_level,
  
  -- Growth indicators
  case 
    when current_month_active_users > active_users * 0.8 then 'Growing'
    when current_month_active_users > active_users * 0.6 then 'Stable'
    when current_month_active_users > active_users * 0.4 then 'Declining'
    else 'Critical Decline'
  end as growth_trend

from firm_health_scoring
order by firm_id, month
