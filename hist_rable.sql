-- models/monitoring/table_snapshot_history.sql
{{ config(
    materialized='incremental',
    unique_key=['snapshot_time', 'table_name', 'schema_name']
) }}

-- Explicit dependencies for all monitored models
-- depends_on: {{ ref('model_1') }}
-- depends_on: {{ ref('model_2') }}
-- depends_on: {{ ref('model_3') }}
-- Add more as needed

{% if is_incremental() %}
    -- In incremental mode, use a WHERE clause in a subquery
    select * from (
        select
            getdate() as snapshot_time, 
            'model_1' as table_name,
            'schema_1' as schema_name,
            (select count(*) from {{ ref('model_1') }}) as row_count
        
        union all
        
        select
            getdate() as snapshot_time,
            'model_2' as table_name,
            'schema_1' as schema_name,
            (select count(*) from {{ ref('model_2') }}) as row_count
        
        union all
        
        select
            getdate() as snapshot_time,
            'model_3' as table_name,
            'schema_2' as schema_name,
            (select count(*) from {{ ref('model_3') }}) as row_count
        
        -- Add more UNION ALL blocks for each additional table
    ) as snapshots
    where snapshot_time > (select max(snapshot_time) from {{ this }})
{% else %}
    -- In full refresh mode, get all data
    select
        getdate() as snapshot_time, 
        'model_1' as table_name,
        'schema_1' as schema_name,
        (select count(*) from {{ ref('model_1') }}) as row_count
    
    union all
    
    select
        getdate() as snapshot_time,
        'model_2' as table_name,
        'schema_1' as schema_name,
        (select count(*) from {{ ref('model_2') }}) as row_count
    
    union all
    
    select
        getdate() as snapshot_time,
        'model_3' as table_name,
        'schema_2' as schema_name,
        (select count(*) from {{ ref('model_3') }}) as row_count
    
    -- Add more UNION ALL blocks for each additional table
{% endif %}
