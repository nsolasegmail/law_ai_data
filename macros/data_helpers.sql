{% macro get_month_start(date_column) %}
  date_trunc('month', {{ date_column }})
{% endmacro %}

{% macro get_month_end(date_column) %}
  date_trunc('month', {{ date_column }}) + interval '1 month' - interval '1 day'
{% endmacro %}

{% macro get_months_between(start_date, end_date) %}
  date_diff('month', {{ start_date }}, {{ end_date }})
{% endmacro %}

{% macro format_date_for_display(date_column) %}
  to_char({{ date_column }}, 'YYYY-MM-DD')
{% endmacro %}
