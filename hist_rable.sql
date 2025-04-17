-- models/monitoring/tagged_tables_snapshot_history.sql
{{ config(
    materialized='incremental',
    unique_key=['snapshot_time', 'table_name', 'schema_name']
) }}

{% set tagged_models = [] %}
{% for node in graph.nodes.values() %}
    {% if node.resource_type == 'model' and node.config.get('tags', [])|select('equalto', 'your_monitoring_tag')|list|length > 0 %}
        {% do tagged_models.append(node) %}
    {% endif %}
{% endfor %}

{% if tagged_models|length == 0 %}
    -- no tagged models found
    select 
        cast(getdate() as timestamp) as snapshot_time,
        cast(null as varchar) as table_name,
        cast(null as varchar) as schema_name,
        cast(0 as bigint) as row_count
    where 1=0
{% else %}
    {% for model in tagged_models %}
        {% if loop.first %}select{% else %}select{% endif %}
            getdate() as snapshot_time,
            '{{ model.name }}' as table_name,
            '{{ model.schema }}' as schema_name,
            (select count(*) from {{ ref(model.name) }}) as row_count
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
{% endif %}

{% if is_incremental() %}
where snapshot_time > (select max(snapshot_time) from {{ this }})
{% endif %}
