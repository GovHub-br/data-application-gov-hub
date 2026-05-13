-- Falha (ou gera warning) se existir algum parlamentar com sigla de partido
-- que não tenha correspondência no seed partidos_logo.

{{ config(severity='warn') }}

with parlamentares as (
    select * from {{ ref('parlamentares') }}
),
partidos_logo as (
    select * from {{ ref('partidos_logo') }}
)

select distinct
    p.sigla_partido
from parlamentares p
left join partidos_logo pl
  on upper(trim(p.sigla_partido)) = upper(trim(pl.sigla))
where p.sigla_partido is not null
  and pl.sigla is null
