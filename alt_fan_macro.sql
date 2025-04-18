-- macros/fanout_detection.sql
{% macro detect_fanout(model_a, model_b, join_column, threshold=1.0) %}

  {# Get row count for model A #}
  {% set model_a_count_query %}
    select count(*) as count from {{ model_a }}
  {% endset %}
  {% set model_a_result = run_query(model_a_count_query) %}
  {% set model_a_count = model_a_result.columns['count'].values()[0] %}
  
  {# Get row count for model B #}
  {% set model_b_count_query %}
    select count(*) as count from {{ model_b }}
  {% endset %}
  {% set model_b_result = run_query(model_b_count_query) %}
  {% set model_b_count = model_b_result.columns['count'].values()[0] %}
  
  {# Get count after joining #}
  {% set join_query %}
    select count(*) as count 
    from {{ model_a }} a
    join {{ model_b }} b on a.{{ join_column }} = b.{{ join_column }}
  {% endset %}
  {% set join_result = run_query(join_query) %}
  {% set join_count = join_result.columns['count'].values()[0] %}
  
  {# Calculate fanout ratios #}
  {% set fanout_ratio_a = join_count / model_a_count %}
  
  {# Log results #}
  {{ log("Model A count: " ~ model_a_count, info=true) }}
  {{ log("Model B count: " ~ model_b_count, info=true) }}
  {{ log("Joined count: " ~ join_count, info=true) }}
  {{ log("Fanout ratio: " ~ fanout_ratio_a, info=true) }}
  
  {# Alert if fanout detected #}
  {% if fanout_ratio_a > threshold %}
    {{ log("⚠️ FANOUT DETECTED: Join between " ~ model_a ~ " and " ~ model_b ~ " produces " ~ fanout_ratio_a ~ "x rows", info=true) }}
    
    {# Identify specific keys causing fanout #}
    {% set fanout_keys_query %}
      with a_counts as (
        select {{ join_column }}, count(*) as a_count
        from {{ model_a }}
        group by {{ join_column }}
      ),
      b_counts as (
        select {{ join_column }}, count(*) as b_count
        from {{ model_b }}
        group by {{ join_column }}
      )
      select 
        a.{{ join_column }},
        a.a_count,
        b.b_count,
        a.a_count * b.b_count as potential_rows
      from a_counts a
      join b_counts b using ({{ join_column }})
      where a.a_count > 1 and b.b_count > 1
      order by potential_rows desc
      limit 10
    {% endset %}
    
    {{ log("Top keys contributing to fanout:", info=true) }}
    {% set fanout_keys = run_query(fanout_keys_query) %}
    {% do fanout_keys.print_table() %}
  {% else %}
    {{ log("✓ No significant fanout detected in join", info=true) }}
  {% endif %}

{% endmacro %}
