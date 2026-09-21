select
    id,
    contrato_id,
    substring(usuario, '(.+) - ') as cpf,
    substring(usuario, '- (.+)') as nome,
    funcao_id,
    descricao_complementar,
    jornada::numeric as jornada,
    trim(
        regexp_replace(
            translate(
                upper(unidade),
                'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ',
                'AAAAAEEEEIIIIOOOOOUUUUC'
            ),
            '[/\-]+|\s+', ' ', 'g'
        )
    ) as unidade,
    replace(replace(salario, '.', ''), ',', '.')::numeric(15, 2) as salario,
    replace(replace(custo, '.', ''), ',', '.')::numeric(15, 2) as custo,
    escolaridade_id,
    to_date(data_inicio, 'YYYY-mm-dd') as data_inicio,
    to_date(data_fim, 'YYYY-mm-dd') as data_fim,
    situacao,
    replace(replace(aux_transporte, '.', ''), ',', '.')::numeric(15, 2) as aux_transporte,
    replace(replace(vale_alimentacao, '.', ''), ',', '.')::numeric(
        15, 2
    ) as vale_alimentacao,
    (dt_ingest || '-03:00')::timestamptz as dt_ingest
from {{ source("compras_gov", "terceirizados") }}
