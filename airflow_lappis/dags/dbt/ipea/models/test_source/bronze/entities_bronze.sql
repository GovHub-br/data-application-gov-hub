{{ config(
    materialized="table",
    schema="test_source",
    tags=["bronze"]
) }}

with
    source_data as (
        select
            cast(id as bigint) as id,
            cast(name as text) as name,
            cast(value as double) as value,
            cast(category as text) as category,
            cast(active as boolean) as active,
            try_cast(dt_ingest as timestamp) as dt_ingest
        from read_parquet(
            '{{ source_path("test_source", "entities") }}/*/*/*/*.parquet'
        )
    )

select *
from source_data
