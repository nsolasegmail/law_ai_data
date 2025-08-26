-- Custom macro tests for Harvey Analytics
-- This file demonstrates how to use the custom testing macros

-- Test 1: Engagement Levels Validation using custom macro
{{ test_engagement_levels_valid('user_engagement', 'engagement_level') }}

-- Test 2: Firm Health Validation using custom macro  
{{ test_firm_health_valid('firm_usage_summary', 'firm_health_status') }}

-- Test 3: Feedback Score Range Validation using custom macro
{{ test_feedback_score_range('user_engagement', 'avg_feedback_score', 1, 5) }}

-- Test 4: Percentage Range Validation using custom macro
{{ test_percentage_range('firm_usage_summary', 'user_adoption_rate', 0, 100) }}

-- Test 5: Non-negative Value Validation using custom macro
{{ test_non_negative('user_engagement', 'total_queries') }}

-- Test 6: Referential Integrity Validation using custom macro
{{ test_referential_integrity('user_engagement', 'firm_id', 'firms', 'id') }}

-- Test 7: Date Consistency Validation using custom macro
{{ test_date_consistency('user_engagement', 'user_created_date', 'month') }}

-- Test 8: Multiple Column Percentage Validation
{{ test_percentage_range('harvey_analytics_dashboard', 'assistant_percent', 0, 100) }}
{{ test_percentage_range('harvey_analytics_dashboard', 'vault_percent', 0, 100) }}
{{ test_percentage_range('harvey_analytics_dashboard', 'workflow_percent', 0, 100) }}

-- Test 9: Multiple Column Non-negative Validation
{{ test_non_negative('firm_usage_summary', 'firm_size') }}
{{ test_non_negative('firm_usage_summary', 'arr_in_thousands') }}
{{ test_non_negative('firm_usage_summary', 'total_users') }}

-- Test 10: Comprehensive Feedback Score Validation
{{ test_feedback_score_range('firm_usage_summary', 'avg_feedback_score', 1, 5) }}
{{ test_feedback_score_range('harvey_analytics_dashboard', 'overall_avg_feedback', 1, 5) }}
