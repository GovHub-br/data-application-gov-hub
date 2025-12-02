select
    id,
    contrato_id,
    funcao_id,
    descricao_complementar,
    jornada::numeric as jornada,
    unidade,
    replace(replace(salario, '.', ''), ',', '.')::numeric(15, 2) as salario,
    replace(replace(custo, '.', ''), ',', '.')::numeric(15, 2) as custo,
    escolaridade_id,
    situacao,
    replace(replace(aux_transporte, '.', ''), ',', '.')::numeric(15, 2) as aux_transporte,
    replace(replace(vale_alimentacao, '.', ''), ',', '.')::numeric(
        15, 2
    ) as vale_alimentacao,
    substring(usuario, '(.+) - ') as cpf,
    substring(usuario, '- (.+)') as nome,
    to_date(data_inicio, 'YYYY-mm-dd') as data_inicio,
    to_date(data_fim, 'YYYY-mm-dd') as data_fim
from {{ source("compras_gov", "terceirizados") }}
