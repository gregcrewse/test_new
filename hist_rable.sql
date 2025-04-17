-- models/monitoring/tagged_tables_snapshot_history.sql
{{ config(
    materialized='incremental',
    unique_key=['snapshot_time', 'table_name', 'schema_name']
) }}

-- Include explicit dependency to the list above
{{ ref('explicit_dependency_list') }}

with monitored_tables as (
    select 
        '{{ var("monitoring_table_1") }}' as table_name,
        '{{ var("monitoring_schema_1") }}' as schema_name
    union all
    select 
        '{{ var("monitoring_table_2") }}' as table_name,
        '{{ var("monitoring_schema_2") }}' as schema_name
    -- Add more tables as needed
),

snapshot_data as (
    select
        getdate() as snapshot_time,
        m.table_name,
        m.schema_name,
        case
            when m.table_name = '{{ var("monitoring_table_1") }}' then 
                (select count(*) from {{ ref(var("monitoring_table_1")) }})
            when m.table_name = '{{ var("monitoring_table_2") }}' then 
                (select count(*) from {{ ref(var("monitoring_table_2")) }})
            -- Add more cases as needed
            else 0
        end as row_count
    from monitored_tables m
)

select * from snapshot_data

{% if is_incremental() %}
where snapshot_time > (select max(snapshot_time) from {{ this }})
{% endif %}
