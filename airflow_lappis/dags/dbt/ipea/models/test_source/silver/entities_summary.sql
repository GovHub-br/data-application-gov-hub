{{ config(
    materialized="table",
    schema="test_source",
    tags=["silver"],
    description="Silver layer - aggregated entities by category"
) }}

with bronze as (
    select *
    from {{ ref('entities_bronze') }}
),

aggregated as (
    select
        category,
        count(*) as total_entities,
        count(distinct id) as unique_entities,
        sum(value) as total_value,
        avg(value) as avg_value,
        min(value) as min_value,
        max(value) as max_value,
        count(case when active = true then 1 end) as active_count,
        max(dt_ingest) as last_ingest
    from bronze
    group by category
)

select *
from aggregated
