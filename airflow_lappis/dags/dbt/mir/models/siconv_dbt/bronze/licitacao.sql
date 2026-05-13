{{ config(materialized="table") }}

with
    licitacao_raw as (
        select
            nullif(id_licitacao, '')::integer as id_licitacao,
            nullif(nr_convenio, '')::integer as nr_convenio,
            nr_licitacao::text as nr_licitacao,
            modalidade_licitacao::text as modalidade_licitacao,
            tp_processo_compra::text as tp_processo_compra,
            tipo_licitacao::text as tipo_licitacao,
            nr_processo_licitacao::text as nr_processo_licitacao,
            to_date(nullif(data_publicacao_licitacao, ''), 'DD/MM/YYYY') as data_publicacao_licitacao,
            to_date(nullif(data_abertura_licitacao, ''), 'DD/MM/YYYY') as data_abertura_licitacao,
            to_date(nullif(data_encerramento_licitacao, ''), 'DD/MM/YYYY') as data_encerramento_licitacao,
            to_date(nullif(data_homologacao_licitacao, ''), 'DD/MM/YYYY') as data_homologacao_licitacao,
            status_licitacao::text as status_licitacao,
            situacao_aceite_processo_execu::text as situacao_aceite_processo_execu,
            sistema_origem::text as sistema_origem,
            situacao_sistema::text as situacao_sistema,
            replace(nullif(valor_licitacao, ''), ',', '.')::numeric(20, 2) as valor_licitacao,
            to_date(nullif(data_analise_aceite, ''), 'DD/MM/YYYY') as data_analise_aceite,
            to_date(nullif(data_envio_analise, ''), 'DD/MM/YYYY') as data_envio_analise
        from {{ source("siconv", "licitacao") }}
    )

select *
from licitacao_raw