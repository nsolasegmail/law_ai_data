{% macro classify_engagement_level(total_queries, active_days) %}
  case
    when {{ total_queries }} >= 20 and {{ active_days }} >= 15 then 'High Frequency User'
    when {{ total_queries }} >= 10 and {{ active_days }} >= 8 then 'Mid Frequency User'
    when {{ total_queries }} >= 5 and {{ active_days }} >= 3 then 'Low Frequency User'
    when {{ total_queries }} >= 1 then 'Occasional User'
    else 'Inactive User'
  end
{% endmacro %}
