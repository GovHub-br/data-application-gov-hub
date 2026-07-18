-- Editais unificados + ano derivado (numero_edital -> nome_edital), validado [2013,2026].
-- (Mantido p/ reconciliação; não é o caminho de ano das cotas.)
select
    nome_edital,
    numero_edital,
    valor_total,
    nome_programa,
    coalesce({{ ano_edital('numero_edital') }}, {{ ano_edital('nome_edital') }}) as ano_edital
from {{ ref('stg_editais') }}
