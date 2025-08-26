-- Custom test to validate data quality across Harvey analytics models
-- This test ensures that our models maintain data integrity and business logic

-- Test 1: Validate that all users in user_engagement have valid engagement levels
with engagement_levels as (
  select distinct engagement_level
  from {{ ref('user_engagement') }}
)
select count(*) as invalid_engagement_levels
from engagement_levels
where engagement_level not in ('Highly Engaged', 'Engaged', 'Moderately Engaged', 'Low Engagement', 'Inactive')

-- Test 2: Validate that firm health status is properly categorized
with firm_health as (
  select distinct firm_health_status
  from {{ ref('firm_usage_summary') }}
)
select count(*) as invalid_firm_health_statuses
from firm_health
where firm_health_status not in ('Excellent Health', 'Good Health', 'Fair Health', 'Needs Attention', 'Critical Issues')

-- Test 3: Validate that feedback scores are within expected range (1-5)
select count(*) as invalid_feedback_scores
from {{ ref('user_engagement') }}
where avg_feedback_score < 1 or avg_feedback_score > 5

-- Test 4: Validate that document counts are non-negative
select count(*) as negative_document_counts
from {{ ref('user_engagement') }}
where total_documents_processed < 0

-- Test 5: Validate that user adoption rates are within 0-100% range
select count(*) as invalid_adoption_rates
from {{ ref('firm_usage_summary') }}
where user_adoption_rate < 0 or user_adoption_rate > 100

-- Test 6: Validate that all months have data
select count(*) as months_without_data
from (
  select distinct month from {{ ref('harvey_analytics_dashboard') }}
  where total_events = 0
)

-- Test 7: Validate that engagement levels align with activity metrics
select count(*) as engagement_activity_mismatch
from {{ ref('user_engagement') }}
where (engagement_level = 'Highly Engaged' and (total_queries < 20 or active_days < 15))
   or (engagement_level = 'Engaged' and (total_queries < 10 or active_days < 8))
   or (engagement_level = 'Moderately Engaged' and (total_queries < 5 or active_days < 3))
   or (engagement_level = 'Low Engagement' and total_queries < 1)
   or (engagement_level = 'Inactive' and total_queries > 0)
