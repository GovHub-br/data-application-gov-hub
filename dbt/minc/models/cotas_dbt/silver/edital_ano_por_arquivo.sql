-- Ano do edital por ANEXO via NOME DO ARQUIVO (3ª camada de datação).
-- O nome do xlsx de origem dos contemplados costuma carregar o ano
-- (ex.: "Contemplados 2024.xlsx", "Edital 0001-2024 - Tamboara.x"). Essa
-- coluna já é trazida pelos stg_contemplados mas não era usada p/ datar.
--
-- Mesma filosofia do edital_ano_por_anexo: agrega por anexo_id e só mantém
-- anexos com ano ÚNICO (having count(distinct ano)=1) — descarta anexo com
-- múltiplos anos no nome (ambíguo). Faixa [2013,2026] (corta lixo tipo 2057).
--
-- Ganho medido (jul/2026): cobre 1.036 anexos vs 477 do método por número do
-- edital → +849 anexos NOVOS. É FALLBACK: menos preciso que o número do edital
-- (nome do arquivo pode ter ano de publicação, defasado ~1 ano do edital), por
-- isso entra depois de nome_edital e anexo_edital no coalesce do contemplados_unif.
with fontes as (
    select
        id_anexo as anexo_id,
        nome_arquivo
    from {{ source('relatorio_gestao', 'planilha_contemplados_lpg') }}
    union all
    select
        id_anexo,
        nome_arquivo
    from {{ source('relatorio_gestao', 'planilha_contemplados_pnab_ciclo_1') }}
),
anos as (
    select
        anexo_id,
        -- retorna TEXT (como ano_edital macro e edital_ano_por_anexo) p/ o
        -- coalesce no contemplados_unif não misturar text com integer.
        case
            when substring(nome_arquivo from '(20[12][0-9])') ~ '^20[12][0-9]$'
                 and substring(nome_arquivo from '(20[12][0-9])')::int between 2013 and 2026
                then substring(nome_arquivo from '(20[12][0-9])')
        end as ano
    from fontes
    where anexo_id is not null
)
select
    anexo_id,
    min(ano) as ano_edital
from anos
where ano is not null
group by anexo_id
having count(distinct ano) = 1
