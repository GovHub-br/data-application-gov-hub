-- Pensões instituídas por servidores do IPEA (consultaPensoesInstituidas do SIAPE).
-- A DAG consulta o SIAPE pelo CPF de cada servidor e grava esse CPF em cpf: aqui ele é
-- o CPF do instituidor, não do pensionista.
-- Granularidade: pensionista por instituidor (id_registro)
with
    pensoes as (
        select
            cpf,
            cpfpensionista,
            nomepensionista,
            matriculapensionista,
            cpfrepresentantelegal,
            nomerepresentantelegal,
            codparentesco,
            nomeparentesco,
            tipopensao,
            nometipopensao,
            naturezapensao,
            cotapensao,
            orgaopensionista,
            banco,
            agencia,
            conta,
            id_registro,
            dt_ingest
        from {{ source("siape", "pensoes_instituidas") }}
    )

select
    id_registro,
    regexp_replace(nullif(trim(cpf), ''), '[^0-9]', '', 'g') as cpf_instituidor,
    regexp_replace(
        nullif(trim(cpfpensionista), ''), '[^0-9]', '', 'g'
    ) as cpf_pensionista,
    nullif(trim(nomepensionista), '') as nome_pensionista,
    nullif(trim(matriculapensionista), '') as matricula_pensionista,
    regexp_replace(
        nullif(trim(cpfrepresentantelegal), ''), '[^0-9]', '', 'g'
    ) as cpf_representante_legal,
    nullif(trim(nomerepresentantelegal), '') as nome_representante_legal,
    nullif(trim(codparentesco), '') as cod_parentesco,
    nullif(trim(nomeparentesco), '') as nome_parentesco,
    nullif(trim(tipopensao), '') as cod_tipo_pensao,
    nullif(trim(nometipopensao), '') as nome_tipo_pensao,
    nullif(trim(naturezapensao), '') as natureza_pensao,
    nullif(trim(cotapensao), '') as cota_pensao,
    nullif(trim(orgaopensionista), '') as orgao_pensionista,
    nullif(trim(banco), '') as banco,
    regexp_replace(nullif(trim(agencia), ''), '[^0-9]', '', 'g') as agencia,
    upper(regexp_replace(nullif(trim(conta), ''), '[^0-9A-Za-z]', '', 'g')) as conta,
    (dt_ingest || '-03:00')::timestamptz as dt_ingest
from pensoes
