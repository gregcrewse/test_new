-- models/monitoring/tagged_tables_snapshot_history.sql
{{ config(
    materialized='incremental',
    unique_key=['snapshot_time', 'table_name', 'schema_name']
) }}

with registry as (
    select * from {{ ref('table_registry') }}
),

row_counts as (
    {% for node in graph.nodes.values() %}
        {% if node.resource_type == 'model' and node.config.get('tags', [])|select('equalto', 'your_monitoring_tag')|list|length > 0 %}
            select
                current_timestamp() as snapshot_time,
                '{{ node.name }}' as table_name,
                '{{ node.schema }}' as schema_name,
                (select count(*) from {{ source(node.schema, node.name) if node.resource_type == 'source' else ref(node.name) }}) as row_count
            from registry
            where table_name = '{{ node.name }}' and schema_name = '{{ node.schema }}'
            {% if not loop.last %} union all {% endif %}
        {% endif %}
    {% endfor %}
)

select * from row_counts

{% if is_incremental() %}
where snapshot_time > (select max(snapshot_time) from {{ this }})
{% endif %}
