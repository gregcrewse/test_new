-- models/monitoring/table_registry.sql
{{ config(
    materialized='table'
) }}

-- this model creates a registry of tables to monitor
{% set tagged_models = [] %}
{% for node in graph.nodes.values() %}
    {% if node.resource_type == 'model' and node.config.get('tags', [])|select('equalto', 'your_monitoring_tag')|list|length > 0 %}
        {% do tagged_models.append(node) %}
    {% endif %}
{% endfor %}

{% if tagged_models|length == 0 %}
    -- no tagged models found - create empty table with correct schema
    select 
        cast(null as varchar) as table_name,
        cast(null as varchar) as schema_name
    where 1=0
{% else %}
    {% for model in tagged_models %}
        {% if loop.first %}select{% else %}select{% endif %}
            '{{ model.name }}' as table_name,
            '{{ model.schema }}' as schema_name
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
{% endif %}
