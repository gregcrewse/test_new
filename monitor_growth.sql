-- models/monitoring/tagged_tables_growth_metrics.sql
with current_snapshot as (
    select
        snapshot_time,
        table_name,
        schema_name,
        row_count
    from {{ ref('tagged_tables_snapshot_history') }}
    where snapshot_time = (select max(snapshot_time) from {{ ref('tagged_tables_snapshot_history') }})
),

historical_snapshots as (
    select 
        snapshot_time,
        table_name,
        schema_name,
        row_count
    from {{ ref('tagged_tables_snapshot_history') }}
    where snapshot_time < (select max(snapshot_time) from {{ ref('tagged_tables_snapshot_history') }})
),

combined_snapshots as (
    select * from historical_snapshots
    union all
    select * from current_snapshot
),

growth_metrics as (
    select
        snapshot_time,
        table_name,
        schema_name,
        row_count,
        lag(row_count) over (partition by table_name, schema_name order by snapshot_time) as previous_row_count,
        lag(snapshot_time) over (partition by table_name, schema_name order by snapshot_time) as previous_snapshot_time,
        datediff(second, previous_snapshot_time, snapshot_time) as seconds_elapsed,
        (row_count - previous_row_count) as rows_added,
        (row_count - previous_row_count) / nullif(seconds_elapsed, 0) as rows_per_second
    from combined_snapshots
),

statistics as (
    select
        table_name,
        schema_name,
        avg(rows_per_second) as avg_rows_per_second,
        stddev(rows_per_second) as stddev_rows_per_second
    from growth_metrics
    where rows_per_second is not null
    group by 1, 2
),

anomaly_detection as (
    select
        g.snapshot_time,
        g.table_name,
        g.schema_name,
        g.row_count,
        g.rows_added,
        g.seconds_elapsed,
        g.rows_per_second,
        s.avg_rows_per_second,
        s.stddev_rows_per_second,
        case
            when g.rows_per_second < (s.avg_rows_per_second - 3 * s.stddev_rows_per_second) 
                then 'SEVERE_SLOWDOWN'
            when g.rows_per_second < (s.avg_rows_per_second - 2 * s.stddev_rows_per_second) 
                then 'SLOWDOWN'
            when g.rows_per_second > (s.avg_rows_per_second + 3 * s.stddev_rows_per_second) 
                then 'SEVERE_SPIKE'
            when g.rows_per_second > (s.avg_rows_per_second + 2 * s.stddev_rows_per_second) 
                then 'SPIKE'
            else 'NORMAL'
        end as status
    from growth_metrics g
    join statistics s on g.table_name = s.table_name and g.schema_name = s.schema_name
    where g.rows_per_second is not null
)

select * from anomaly_detection
order by snapshot_time desc, table_name
