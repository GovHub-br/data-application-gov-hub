-- Teste singular: nenhum estagiário em forca_trabalho pode ter nome_cargo nulo.
-- Retorna linhas que violam a regra; dbt falha se o resultado não for vazio.
select
    cpf,
    nome_situacao_funcional,
    nome_cargo
from {{ ref("forca_trabalho") }}
where
    upper(nome_situacao_funcional) = 'ESTAGIARIO SIGEPE'
    and nullif(trim(nome_cargo), '') is null
