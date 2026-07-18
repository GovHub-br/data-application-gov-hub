-- Ano do edital por ANEXO (PNAB): das abas de definição de edital, que trazem
-- o campo "número do edital" (mais completo que o nome no contemplado).
-- Só mantém anexos com ano ÚNICO (consistente) — descarta anexo multi-ano (ambíguo).
{% set ag  = source('transferegov_fundo_a_fundo', 'raw_pnab_acoes_gerais') %}
{% set acv = source('transferegov_fundo_a_fundo', 'raw_pnab_acoes_cultura_viva') %}
with fontes as (
    select
        substring(id_anexo from 'anexo_([0-9]+)')                                      as anexo_id,
        {{ coalesce_por_nome(ag, ['número do edital', 'número e título do edital']) }}  as num_edital
    from {{ ag }}
    union all
    select
        substring(id_anexo from 'anexo_([0-9]+)'),
        {{ coalesce_por_nome(acv, ['número e título do edital', 'número do edital']) }}
    from {{ acv }}
),
anos as (
    select anexo_id, {{ ano_edital('num_edital') }} as ano
    from fontes
)
select
    anexo_id,
    min(ano) as ano_edital
from anos
where ano is not null
group by anexo_id
having count(distinct ano) = 1
