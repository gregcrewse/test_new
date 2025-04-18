{% macro check_for_fanout(model, reference_model, join_column, reference_column) %}
    
    {% set model_count_query %}
        select count(*) as row_count from {{ model }}
    {% endset %}
    
    {% set reference_count_query %}
        select count(*) as row_count from {{ reference_model }}
    {% endset %}
    
    {% set join_count_query %}
        select count(*) as row_count 
        from {{ model }} a
        join {{ reference_model }} b
        on a.{{ join_column }} = b.{{ reference_column }}
    {% endset %}
    
    {% set model_count = run_query(model_count_query).columns[0].values()[0] %}
    {% set reference_count = run_query(reference_count_query).columns[0].values()[0] %}
    {% set join_count = run_query(join_count_query).columns[0].values()[0] %}
    
    {% set fanout_ratio = join_count / model_count %}
    
    {% if fanout_ratio > 1 %}
        {{ log("⚠️ FANOUT DETECTED: Join produces " ~ fanout_ratio ~ "x the rows in " ~ model, info=True) }}
    {% endif %}
    
{% endmacro %}
