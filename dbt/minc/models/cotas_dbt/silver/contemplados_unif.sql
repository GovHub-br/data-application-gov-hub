-- Contemplados unificados LPG + PNAB (núcleo do lado-valor).
-- ANO (edital-year): 1) nome_edital "NN/AAAA"; 2) número do edital por ANEXO
-- (abas de definição PNAB); 3) NOVO fallback = ano no NOME DO ARQUIVO por anexo.
-- Todos validados [2013,2026]. anexo_id propagado até aqui p/ rastreio no fct.
--
-- LIMPEZA DE LIXO (linhas-fantasma da extração de planilha): as planilhas PNAB
-- trazem linhas que NÃO são pagamentos individuais mas carregam valor e por isso
-- vazam pelo filtro do bronze (que só corta linha sem doc E sem valor). São:
--   • rodapé de subtotal ("TOTAL DE RECURSOS APLICADOS:") — o total da planilha,
--     que somado como pagamento DUPLICA o dinheiro (medido: ~R$492M de fantasma,
--     maior que o total real do PNAB ~R$447M no painel oficial);
--   • texto de instrução do template ("Insira informações na aba ...").
-- Corte por `nome_edital` (onde esses textos caem via coalesce). Feito aqui na
-- silver (não no bronze, que fica raw-fiel) — cobre fct + golds num único ponto.
-- ATENÇÃO: NÃO cortar nome_edital = 'nan' — isso é pagamento REAL sem edital
-- identificado (vira sem_ano no denominador), não lixo. Cortá-lo removeria valor
-- legítimo. Só subtotal e instrução são lixo de fato.
with contemplados_raw as (
    select * from {{ ref('stg_contemplados_lpg') }}
    union all
    select * from {{ ref('stg_contemplados_pnab') }}
),
contemplados as (
    select *
    from contemplados_raw
    where upper(coalesce(nome_edital, '')) not like 'TOTAL DE RECURSOS%'
      and lower(coalesce(nome_edital, '')) not like 'insira informa%'
),
com_ano as (
    select
        c.identificador_unico,
        c.chave_anonimizada,
        c.valor_pago_num,
        c.nome_edital,
        c.origem,
        c.nome_programa,
        c.anexo_id,
        c.nome_arquivo,
        {{ ano_edital('c.nome_edital') }} as ano_nome,
        ea.ano_edital                     as ano_anexo,
        ar.ano_edital                     as ano_arquivo
    from contemplados c
    left join {{ ref('edital_ano_por_anexo') }} ea
        on c.anexo_id = ea.anexo_id
    left join {{ ref('edital_ano_por_arquivo') }} ar
        on c.anexo_id = ar.anexo_id
)
select
    identificador_unico,
    chave_anonimizada,
    valor_pago_num,
    nome_edital,
    origem,
    nome_programa,
    anexo_id,
    nome_arquivo,
    coalesce(ano_nome, ano_anexo, ano_arquivo) as ano_final,
    case
        when ano_nome    is not null then 'nome_edital'
        when ano_anexo   is not null then 'anexo_edital'
        when ano_arquivo is not null then 'nome_arquivo'
        else 'sem_ano'
    end as origem_ano
from com_ano
