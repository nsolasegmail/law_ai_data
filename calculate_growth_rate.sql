{% macro calculate_growth_rate(current_value, previous_value) %}
  case 
    when {{ previous_value }} > 0 
    then round((({{ current_value }} - {{ previous_value }})::float / {{ previous_value }} * 100), 2)
    else null 
  end
{% endmacro %}
