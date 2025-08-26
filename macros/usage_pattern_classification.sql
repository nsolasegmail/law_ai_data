{% macro classify_usage_pattern(assistant_queries, vault_queries, workflow_queries) %}
  case 
    when {{ assistant_queries }} > {{ vault_queries }} and {{ assistant_queries }} > {{ workflow_queries }} then 'Assistant-Focused'
    when {{ vault_queries }} > {{ assistant_queries }} and {{ vault_queries }} > {{ workflow_queries }} then 'Vault-Focused'
    when {{ workflow_queries }} > {{ assistant_queries }} and {{ workflow_queries }} > {{ vault_queries }} then 'Workflow-Focused'
    when {{ assistant_queries }} = {{ vault_queries }} and {{ vault_queries }} = {{ workflow_queries }} then 'Balanced Usage'
    else 'Mixed Usage'
  end
{% endmacro %}
