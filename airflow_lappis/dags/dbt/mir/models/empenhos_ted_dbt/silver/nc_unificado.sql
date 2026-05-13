{{ config(materialized="table") }}

with
	nc_tesouro as (
		select
			*
		from {{ ref("nc_tesouro_mir") }}
	),

    programa_por_ptres as (
        select
            ptres,
            programa_governo,
            programa_governo_descricao,
            acao_governo,
            plano_orcamentario_descricao_6
        from (
            select
                ptres,
                programa_governo,
                programa_governo_descricao,
                acao_governo,
                plano_orcamentario_descricao_6,
                row_number() over (partition by trim(ptres) order by ptres) as rn
            from {{ ref("pf_ptres") }}
        ) subquery
        where rn = 1
    )

select
	coalesce(t.programa_governo, pp.programa_governo) as programa_governo,
	coalesce(t.programa_governo_descricao, pp.programa_governo_descricao) as programa_governo_descricao,
	coalesce(t.acao_governo, pp.acao_governo) as acao_governo,
	coalesce(t.acao_governo_descricao, pp.plano_orcamentario_descricao_6) as acao_governo_descricao,
	t.nc,
	t.nc_transferencia,
	t.nc_fonte_recursos,
	t.nc_fonte_recursos_descricao,
	t.ptres,
	t.nc_evento_descricao,
	t.nc_ug_responsavel,
	t.nc_ug_responsavel_descricao,
	t.nc_natureza_despesa,
	t.nc_natureza_despesa_descricao,
	t.nc_plano_interno,
	t.nc_plano_interno_descricao1,
	t.favorecido_doc,
	t.favorecido_doc_descricao,
	t.favorecido_municipio,
	t.favorecido_municipio_descricao,
	t.valor_celula,
	t.movimento_liquido_moeda_origem,
	t.dt_ingest,
	t.descricao,
	t.nc_plano_interno_descricao2,
	t.nc_evento,
	t.nc_item_detalhamento,
	t.emissao_dia,
	t.emissao_mes,
	t.emissao_ano,
	t.ro,
	t.dc,
	t.total_lista,
	t.esfera_orcamentaria_codigo,
	t.esfera_orcamentaria_nome
from nc_tesouro t
left join programa_por_ptres pp on trim(pp.ptres) = trim(t.ptres)