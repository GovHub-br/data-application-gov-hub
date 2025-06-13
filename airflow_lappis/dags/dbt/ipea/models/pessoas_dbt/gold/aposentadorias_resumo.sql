-- Tabela contendo os aposentados e tempo de serviço
-- Granularidade: cpf
with

    aposentados_extract as (
        select distinct
            cpf,
            nome_pessoa,
            dt_ocorr_ingresso_serv_publico,
            dt_ocorr_aposentadoria,
            to_date(
                to_char(dt_ocorr_aposentadoria, 'YYYY-MM'), 'YYYY-MM'
            ) as mes_aposentadoria,
            nome_situacao_funcional,
            nome_ocorr_aposentadoria,
            nome_cargo,
            sigla_nivel_cargo,
            cod_classe || '-' || cod_padrao as classe_padrao
        from {{ ref("servidores_detalhados") }} sd
        where dt_ocorr_aposentadoria is not null
    )

select
    *,
    age(dt_ocorr_aposentadoria, dt_ocorr_ingresso_serv_publico) as age,
    extract(
        year from age(dt_ocorr_aposentadoria, dt_ocorr_ingresso_serv_publico)
    ) as diff_anos,
    extract(
        month from age(dt_ocorr_aposentadoria, dt_ocorr_ingresso_serv_publico)
    ) as diff_meses,
    extract(
        days from age(dt_ocorr_aposentadoria, dt_ocorr_ingresso_serv_publico)
    ) as diff_dias
from aposentados_extract
