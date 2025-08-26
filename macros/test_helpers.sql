{% macro test_engagement_levels_valid(model, column_name) %}
  {% set engagement_levels = ['Highly Engaged', 'Engaged', 'Moderately Engaged', 'Low Engagement', 'Inactive'] %}
  
  with validation_errors as (
    select distinct {{ column_name }}
    from {{ model }}
    where {{ column_name }} not in ({% for level in engagement_levels %}'{{ level }}'{% if not loop.last %},{% endif %}{% endfor %})
  )
  select count(*) as error_count
  from validation_errors
{% endmacro %}

{% macro test_firm_health_valid(model, column_name) %}
  {% set health_statuses = ['Excellent Health', 'Good Health', 'Fair Health', 'Needs Attention', 'Critical Issues'] %}
  
  with validation_errors as (
    select distinct {{ column_name }}
    from {{ model }}
    where {{ column_name }} not in ({% for status in health_statuses %}'{{ status }}'{% if not loop.last %},{% endif %}{% endfor %})
  )
  select count(*) as error_count
  from validation_errors
{% endmacro %}

{% macro test_feedback_score_range(model, column_name, min_score=1, max_score=5) %}
  select count(*) as error_count
  from {{ model }}
  where {{ column_name }} < {{ min_score }} or {{ column_name }} > {{ max_score }}
{% endmacro %}

{% macro test_percentage_range(model, column_name, min_percent=0, max_percent=100) %}
  select count(*) as error_count
  from {{ model }}
  where {{ column_name }} < {{ min_percent }} or {{ column_name }} > {{ max_percent }}
{% endmacro %}

{% macro test_non_negative(model, column_name) %}
  select count(*) as error_count
  from {{ model }}
  where {{ column_name }} < 0
{% endmacro %}

{% macro test_date_consistency(model, start_date, end_date) %}
  select count(*) as error_count
  from {{ model }}
  where {{ start_date }} > {{ end_date }}
{% endmacro %}

{% macro test_referential_integrity(model, foreign_key, referenced_model, referenced_key) %}
  select count(*) as error_count
  from {{ model }} m
  left join {{ referenced_model }} r on m.{{ foreign_key }} = r.{{ referenced_key }}
  where r.{{ referenced_key }} is null
{% endmacro %}
