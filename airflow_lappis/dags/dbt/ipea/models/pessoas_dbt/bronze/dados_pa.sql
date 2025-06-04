with
    dados_pa as (
        select
            agenciabeneficiario,
            bancobeneficiario,
            codorgao,
            contabeneficiario,
            cpfbeneficiario,
            matricula,
            nomebeneficiario,
            valorultimapensao,
            cpf_servidor,
            codvinculoservidor,
            nomealimentado,
            nomevinculoservidor
        from {{ source("siape", "dados_pa") }}
    )

select
    regexp_replace(
        nullif(trim(agenciabeneficiario), ''), '[^0-9]', '', 'g'
    ) as agencia_beneficiario,
    nullif(trim(bancobeneficiario), '') as banco_beneficiario,
    nullif(trim(codorgao), '') as cod_orgao,
    upper(
        regexp_replace(nullif(trim(contabeneficiario), ''), '[^0-9A-Za-z]', '', 'g')
    ) as conta_beneficiario,
    regexp_replace(
        nullif(trim(cpfbeneficiario), ''), '[^0-9]', '', 'g'
    ) as cpf_beneficiario,
    nullif(trim(matricula), '') as matricula_servidor,
    nullif(trim(nomebeneficiario), '') as nome_beneficiario,
    nullif(trim(valorultimapensao), '') as valor_ultima_pensao,
    regexp_replace(nullif(trim(cpf_servidor), ''), '[^0-9]', '', 'g') as cpf_servidor,
    nullif(trim(codvinculoservidor), '') as cod_vinculo_servidor,
    nullif(trim(nomealimentado), '') as nome_alimentado,
    nullif(trim(nomevinculoservidor), '') as nome_vinculo_servidor
from dados_pa
