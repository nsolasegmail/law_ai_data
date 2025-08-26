{% macro classify_firm_health(avg_feedback_score, queries_per_user) %}
  case 
    when {{ avg_feedback_score }} >= 4.5 and {{ queries_per_user }} >= 10 then 'Excellent Health'
    when {{ avg_feedback_score }} >= 4.0 and {{ queries_per_user }} >= 5 then 'Good Health'
    when {{ avg_feedback_score }} >= 3.5 and {{ queries_per_user }} >= 2 then 'Fair Health'
    when {{ avg_feedback_score }} >= 3.0 then 'Needs Attention'
    else 'Critical Issues'
  end
{% endmacro %}
