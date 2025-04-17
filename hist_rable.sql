-- models/monitoring/tagged_tables_snapshot_history.sql
{{ config(
    materialized='incremental',
    unique_key=['snapshot_time', 'table_name', 'schema_name']
) }}

-- All tag dependencies need to be listed explicitly here
{%- set tagged_models_dependencies = [] -%}
{%- for node in graph.nodes.values() -%}
    {%- if node.resource_type == 'model' and node.config.get('tags', [])|select('equalto', 'your_monitoring_tag')|list|length > 0 -%}
        {%- do tagged_models_dependencies.append(node.name) -%}
    {%- endif -%}
{%- endfor -%}

{%- if execute -%}
    {%- for dep in tagged_models_dependencies -%}
    -- depends on: {{ ref(dep) }}
    {%- endfor -%}
{%- endif -%}

with snapshot_data as (
    {% for node in graph.nodes.values() %}
        {% if node.resource_type == 'model' and node.config.get('tags', [])|select('equalto', 'your_monitoring_tag')|list|length > 0 %}
            {% if not loop.first %}union all{% endif %}
            select
                getdate() as snapshot_time,
                '{{ node.name }}' as table_name,
                '{{ node.schema }}' as schema_name,
                (select count(*) from {{ ref(node.name) }}) as row_count
        {% endif %}
    {% endfor %}
)

select * from snapshot_data

{% if is_incremental() %}
where snapshot_time > (select max(snapshot_time) from {{ this }})
{% endif %}
