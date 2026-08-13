-- Ano do edital por ANEXO (PNAB): das abas de definição de edital, que trazem
-- o campo "número do edital" (mais completo que o nome no contemplado).
-- Só mantém anexos com ano ÚNICO (consistente) — descarta anexo multi-ano (ambíguo).
-- As duas abas de definição de edital viraram fatias da mesma tabela,
-- separadas por tabela_origem. id_anexo agora é o id do anexo direto — antes
-- era o nome do arquivo ("anexo_123_..."), de onde o id precisava ser extraído.
{% set editais = source('relatorio_gestao', 'planilha_editais_pnab_ciclo_1') %}
with fontes as (
    select
        id_anexo                                                                            as anexo_id,
        {{ coalesce_por_nome(editais, ['número do edital', 'número e título do edital']) }}  as num_edital
    from {{ editais }}
    where tabela_origem = 'raw_pnab_acoes_gerais'
    union all
    select
        id_anexo,
        {{ coalesce_por_nome(editais, ['número e título do edital', 'número do edital']) }}
    from {{ editais }}
    where tabela_origem = 'raw_pnab_acoes_cultura_viva'
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
