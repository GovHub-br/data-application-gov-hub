-- Time spine do MetricFlow — PRÉ-REQUISITO de infraestrutura do dbt Semantic
-- Layer: qualquer projeto que declare `semantic_models`/`metrics` precisa deste
-- model, mesmo que nenhuma métrica seja baseada em tempo (sem ele o `dbt parse`
-- falha com "The semantic layer requires a 'metricflow_time_spine' model").
--
-- Grão: 1 linha por dia. A coluna DEVE se chamar `date_day` (convenção do
-- MetricFlow na spec legada, dbt-core 1.x).
--
-- Faixa: 2010–2035. Cobre com folga o domínio real dos dados de fomento
-- (editais validados em [2013, 2026] pela macro `ano_edital`), com margem para
-- anos futuros sem precisar reconstruir o spine.
{{ config(materialized='table') }}

select
    cast(dia as date) as date_day
from generate_series(
    '2010-01-01'::date,
    '2035-12-31'::date,
    interval '1 day'
) as dia
