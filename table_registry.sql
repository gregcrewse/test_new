-- models/monitoring/table_registry.sql
{{ config(
    materialized='table'
) }}

-- this model creates a registry of tables to monitor
with tagged_tables as (
    {% for node in graph.nodes.values() %}
        {% if node.resource_type == 'model' and node.config.get('tags', [])|select('equalto', 'your_monitoring_tag')|list|length > 0 %}
            select 
                '{{ node.name }}' as table_name,
                '{{ node.schema }}' as schema_name
            {% if not loop.last %} union all {% endif %}
        {% endif %}
    {% endfor %}
)

select * from tagged_tables
