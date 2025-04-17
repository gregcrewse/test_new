-- models/monitoring/tagged_tables_growth_metrics.sql

WITH 

-- Get metadata about all tables with specific tag
tagged_tables AS (
    {% for node in graph.nodes.values() %}
        {% if node.resource_type == 'model' and node.config.get('tags', [])|select('equalto', 'your_monitoring_tag')|list|length > 0 %}
            SELECT 
                '{{ node.name }}' AS table_name,
                '{{ node.schema }}' AS schema_name
            {% if not loop.last %} UNION ALL {% endif %}
        {% endif %}
    {% endfor %}
),

-- Get the current snapshot of table row counts
current_snapshot AS (
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
),

-- Historical snapshots from previous runs
historical_snapshots AS (
    SELECT 
        snapshot_time,
        table_name,
        schema_name,
        row_count
    FROM {{ ref('tagged_tables_snapshot_history') }}
),

-- Combine historical with current
combined_snapshots AS (
    SELECT * FROM historical_snapshots
    UNION ALL
    SELECT * FROM current_snapshot
),

-- Calculate growth rates between snapshots
growth_metrics AS (
    SELECT
        snapshot_time,
        table_name,
        schema_name,
        row_count,
        LAG(row_count) OVER (PARTITION BY table_name, schema_name ORDER BY snapshot_time) AS previous_row_count,
        LAG(snapshot_time) OVER (PARTITION BY table_name, schema_name ORDER BY snapshot_time) AS previous_snapshot_time,
        DATEDIFF('SECOND', previous_snapshot_time, snapshot_time) AS seconds_elapsed,
        (row_count - previous_row_count) AS rows_added,
        (row_count - previous_row_count) / NULLIF(seconds_elapsed, 0) AS rows_per_second
    FROM combined_snapshots
),

-- Calculate statistical thresholds based on historical patterns
statistics AS (
    SELECT
        table_name,
        schema_name,
        AVG(rows_per_second) AS avg_rows_per_second,
        STDDEV(rows_per_second) AS stddev_rows_per_second
    FROM growth_metrics
    WHERE rows_per_second IS NOT NULL
    GROUP BY 1, 2
),

-- Flag anomalies based on thresholds
anomaly_detection AS (
    SELECT
        g.snapshot_time,
        g.table_name,
        g.schema_name,
        g.row_count,
        g.rows_added,
        g.seconds_elapsed,
        g.rows_per_second,
        s.avg_rows_per_second,
        s.stddev_rows_per_second,
        CASE
            WHEN g.rows_per_second < (s.avg_rows_per_second - 3 * s.stddev_rows_per_second) 
                THEN 'SEVERE_SLOWDOWN'
            WHEN g.rows_per_second < (s.avg_rows_per_second - 2 * s.stddev_rows_per_second) 
                THEN 'SLOWDOWN'
            WHEN g.rows_per_second > (s.avg_rows_per_second + 3 * s.stddev_rows_per_second) 
                THEN 'SEVERE_SPIKE'
            WHEN g.rows_per_second > (s.avg_rows_per_second + 2 * s.stddev_rows_per_second) 
                THEN 'SPIKE'
            ELSE 'NORMAL'
        END AS status
    FROM growth_metrics g
    JOIN statistics s ON g.table_name = s.table_name AND g.schema_name = s.schema_name
    WHERE g.rows_per_second IS NOT NULL
)

SELECT * FROM anomaly_detection
ORDER BY snapshot_time DESC, table_name
