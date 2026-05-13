{{ config(materialized="table") }}

with
    convenio_raw as (
        select
            nullif(nr_convenio, '')::integer as nr_convenio,
            nullif(id_proposta, '')::integer as id_proposta,
            nullif(dia, '')::integer as dia,
            nullif(mes, '')::integer as mes,
            nullif(ano, '')::integer as ano,
            to_date(nullif(dia_assin_conv, ''), 'DD/MM/YYYY') as dia_assin_conv,
            sit_convenio::text as sit_convenio,
            subsituacao_conv::text as subsituacao_conv,
            situacao_publicacao::text as situacao_publicacao,
            instrumento_ativo::text as instrumento_ativo,
            ind_opera_obtv::text as ind_opera_obtv,
            nr_processo::text as nr_processo,
            nullif(ug_emitente, '')::integer as ug_emitente,
            to_date(nullif(dia_publ_conv, ''), 'DD/MM/YYYY') as dia_publ_conv,
            to_date(nullif(dia_inic_vigenc_conv, ''), 'DD/MM/YYYY') as dia_inic_vigenc_conv,
            to_date(nullif(dia_fim_vigenc_conv, ''), 'DD/MM/YYYY') as dia_fim_vigenc_conv,
            to_date(nullif(dia_fim_vigenc_original_conv, ''), 'DD/MM/YYYY') as dia_fim_vigenc_original_conv,
            nullif(dias_prest_contas, '')::integer as dias_prest_contas,
            to_date(nullif(dia_limite_prest_contas, ''), 'DD/MM/YYYY') as dia_limite_prest_contas,
            to_date(nullif(data_suspensiva, ''), 'DD/MM/YYYY') as data_suspensiva,
            to_date(nullif(data_retirada_suspensiva, ''), 'DD/MM/YYYY') as data_retirada_suspensiva,
            nullif(dias_clausula_suspensiva, '')::integer as dias_clausula_suspensiva,
            situacao_contratacao::text as situacao_contratacao,
            ind_assinado::text as ind_assinado,
            motivo_suspensao::text as motivo_suspensao,
            ind_foto::text as ind_foto,
            nullif(qtde_convenios, '')::integer as qtde_convenios,
            nullif(qtd_ta, '')::integer as qtd_ta,
            nullif(qtd_prorroga, '')::integer as qtd_prorroga,
            replace(nullif(vl_global_conv, ''), ',', '.')::numeric(15, 2) as vl_global_conv,
            replace(nullif(vl_repasse_conv, ''), ',', '.')::numeric(15, 2) as vl_repasse_conv,
            replace(nullif(vl_contrapartida_conv, ''), ',', '.')::numeric(15, 2) as vl_contrapartida_conv,
            replace(nullif(vl_empenhado_conv, ''), ',', '.')::numeric(15, 2) as vl_empenhado_conv,
            replace(nullif(vl_desembolsado_conv, ''), ',', '.')::numeric(15, 2) as vl_desembolsado_conv,
            replace(nullif(vl_saldo_reman_tesouro, ''), ',', '.')::numeric(15, 2) as vl_saldo_reman_tesouro,
            replace(nullif(vl_saldo_reman_convenente, ''), ',', '.')::numeric(15, 2) as vl_saldo_reman_convenente,
            replace(nullif(vl_rendimento_aplicacao, ''), ',', '.')::numeric(15, 2) as vl_rendimento_aplicacao,
            replace(nullif(vl_ingresso_contrapartida, ''), ',', '.')::numeric(15, 2) as vl_ingresso_contrapartida,
            replace(nullif(vl_saldo_conta, ''), ',', '.')::numeric(15, 2) as vl_saldo_conta,
            replace(nullif(valor_global_original_conv, ''), ',', '.')::numeric(15, 2) as valor_global_original_conv
        from {{ source("siconv", "convenio") }}
    )

select *
from convenio_raw