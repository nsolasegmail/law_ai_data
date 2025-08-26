{% macro classify_quality_level(avg_feedback_score) %}
  case 
    when {{ avg_feedback_score }} >= 4.5 then 'High Quality'
    when {{ avg_feedback_score }} >= 3.5 then 'Good Quality'
    when {{ avg_feedback_score }} >= 2.5 then 'Average Quality'
    else 'Needs Improvement'
  end
{% endmacro %}
