-- Comprehensive testing for Harvey Analytics models using macros
-- This file tests data quality, business logic, and model consistency

-- Test 1: Engagement Level Classification Test
-- Validates that engagement levels are properly classified based on activity metrics
with engagement_test as (
  select 
    user_id,
    month,
    total_queries,
    active_days,
    engagement_level
  from {{ ref('user_engagement') }}
  where month >= '2024-01-01'  -- Test recent data
)
select count(*) as engagement_classification_errors
from engagement_test
where (engagement_level = 'Highly Engaged' and (total_queries < 20 or active_days < 15))
   or (engagement_level = 'Engaged' and (total_queries < 10 or active_days < 8))
   or (engagement_level = 'Moderately Engaged' and (total_queries < 5 or active_days < 3))
   or (engagement_level = 'Low Engagement' and total_queries < 1)
   or (engagement_level = 'Inactive' and total_queries > 0)

-- Test 2: Usage Pattern Classification Test
-- Validates that usage patterns are correctly identified based on event type preferences
with usage_pattern_test as (
  select 
    user_id,
    month,
    assistant_queries,
    vault_queries,
    workflow_queries,
    usage_pattern
  from {{ ref('user_engagement') }}
  where month >= '2024-01-01'
)
select count(*) as usage_pattern_errors
from usage_pattern_test
where (usage_pattern = 'Assistant-Focused' and (assistant_queries <= vault_queries or assistant_queries <= workflow_queries))
   or (usage_pattern = 'Vault-Focused' and (vault_queries <= assistant_queries or vault_queries <= workflow_queries))
   or (usage_pattern = 'Workflow-Focused' and (workflow_queries <= assistant_queries or workflow_queries <= vault_queries))

-- Test 3: Firm Health Status Test
-- Validates that firm health status aligns with feedback scores and usage metrics
with firm_health_test as (
  select 
    firm_id,
    month,
    avg_feedback_score,
    queries_per_user,
    firm_health_status
  from {{ ref('firm_usage_summary') }}
  where month >= '2024-01-01'
)
select count(*) as firm_health_errors
from firm_health_test
where (firm_health_status = 'Excellent Health' and (avg_feedback_score < 4.5 or queries_per_user < 10))
   or (firm_health_status = 'Good Health' and (avg_feedback_score < 4.0 or queries_per_user < 5))
   or (firm_health_status = 'Fair Health' and (avg_feedback_score < 3.5 or queries_per_user < 2))
   or (firm_health_status = 'Needs Attention' and avg_feedback_score < 3.0)

-- Test 4: Data Consistency Test
-- Validates that metrics are consistent across related fields
with consistency_test as (
  select 
    firm_id,
    month,
    total_queries,
    assistant_queries + vault_queries + workflow_queries as calculated_total_queries,
    total_users,
    active_users
  from {{ ref('firm_usage_summary') }}
  where month >= '2024-01-01'
)
select count(*) as consistency_errors
from consistency_test
where total_queries != calculated_total_queries
   or active_users > total_users
   or total_users < 0

-- Test 5: Growth Rate Validation Test
-- Validates that growth rates are within reasonable bounds
with growth_rate_test as (
  select 
    month,
    user_growth_rate_percent,
    firm_growth_rate_percent
  from {{ ref('harvey_analytics_dashboard') }}
  where month >= '2024-01-01'
)
select count(*) as growth_rate_errors
from growth_rate_test
where (user_growth_rate_percent is not null and (user_growth_rate_percent < -100 or user_growth_rate_percent > 1000))
   or (firm_growth_rate_percent is not null and (firm_growth_rate_percent < -100 or firm_growth_rate_percent > 1000))

-- Test 6: Date Logic Test
-- Validates that date relationships make sense
with date_logic_test as (
  select 
    user_id,
    month,
    user_created_date,
    months_since_creation
  from {{ ref('user_engagement') }}
  where month >= '2024-01-01'
)
select count(*) as date_logic_errors
from date_logic_test
where months_since_creation < 0
   or month < user_created_date

-- Test 7: Percentage Validation Test
-- Validates that percentage fields sum to 100% where applicable
with percentage_test as (
  select 
    month,
    assistant_percent,
    vault_percent,
    workflow_percent
  from {{ ref('harvey_analytics_dashboard') }}
  where month >= '2024-01-01'
)
select count(*) as percentage_errors
from percentage_test
where abs((assistant_percent + vault_percent + workflow_percent) - 100) > 1  -- Allow 1% tolerance

-- Test 8: Feedback Score Range Test
-- Validates that feedback scores are within expected 1-5 range
with feedback_test as (
  select 
    user_id,
    month,
    avg_feedback_score
  from {{ ref('user_engagement') }}
  where month >= '2024-01-01'
)
select count(*) as feedback_score_errors
from feedback_test
where avg_feedback_score < 1 or avg_feedback_score > 5

-- Test 9: Document Count Validation Test
-- Validates that document counts are non-negative and reasonable
with document_test as (
  select 
    user_id,
    month,
    total_documents_processed,
    avg_documents_per_query
  from {{ ref('user_engagement') }}
  where month >= '2024-01-01'
)
select count(*) as document_count_errors
from document_test
where total_documents_processed < 0
   or avg_documents_per_query < 0
   or (total_documents_processed > 0 and avg_documents_per_query = 0)

-- Test 10: Model Referential Integrity Test
-- Validates that foreign keys reference valid records
with referential_test as (
  select 
    ue.firm_id,
    ue.user_id
  from {{ ref('user_engagement') }} ue
  left join {{ ref('firms') }} f on ue.firm_id = f.id
  left join {{ ref('users') }} u on ue.user_id = u.id
  where ue.month >= '2024-01-01'
)
select count(*) as referential_integrity_errors
from referential_test
where firm_id is null or user_id is null
