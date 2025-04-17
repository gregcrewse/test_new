-- models/monitoring/tagged_tables_snapshot_history.sql

{{ config(
    materialized='incremental',
    unique_key=['snapshot_time', 'table_name', 'schema_name']
) }}

-- Collect snapshots for all tagged tables
{% for node in graph.nodes.values() %}
    {% if node.resource_type == 'model' and node.config.get('tags', [])|select('equalto', 'your_monitoring_tag')|list|length > 0 %}
        SELECT 
            current_timestamp() AS snapshot_time,
            '{{ node.name }}' AS table_name,
            '{{ node.schema }}' AS schema_name,
            (SELECT COUNT(*) FROM {{ ref(node.name) }}) AS row_count
        {% if not loop.last %} UNION ALL {% endif %}
    {% endif %}
{% endfor %}

{% if is_incremental() %}
    -- Avoid duplicate snapshots within a short time period
    WHERE snapshot_time > (SELECT MAX(snapshot_time) FROM {{ this }})
{% endif %}
