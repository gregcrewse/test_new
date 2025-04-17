-- models/monitoring/explicit_dependency_list.sql
{{ config(
    materialized='ephemeral'
) }}

-- Explicitly list all dependencies here
-- depends_on: {{ ref('model_1') }}
-- depends_on: {{ ref('model_2') }}
-- depends_on: {{ ref('model_3') }}
-- Add all models you want to monitor

select 1 as dummy
