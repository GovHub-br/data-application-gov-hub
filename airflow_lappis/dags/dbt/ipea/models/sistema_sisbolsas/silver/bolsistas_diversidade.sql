with
    bolsistas_base as (
        select distinct b.co_usuario, b.co_selecao
        from {{ ref("sisbolsas_tb_bolsista") }} as b
    ),

    unidade_por_bolsista as (
        select distinct bb.co_usuario, coalesce(u.ds_sigla, 'SEM_UNIDADE') as unidade
        from bolsistas_base as bb
        left join {{ ref("sisbolsas_tb_selecao") }} as s on bb.co_selecao = s.co_selecao
        left join
            {{ ref("sisbolsas_tb_chapubli_unidade") }} as cu
            on s.co_chamada_publica = cu.co_chamada_publica
        left join {{ ref("sisbolsas_tb_unidade") }} as u on cu.co_unidade = u.co_unidade
    ),

    demografia as (
        select
            dp.co_usuario,
            case when upper(trim(dp.tp_sexo)) = 'F' then 1 else 0 end as is_mulher,
            case
                when dp.co_etnia is null or upper(trim(dp.co_etnia)) = 'NAN'
                then 0
                when split_part(trim(dp.co_etnia), '.', 1) in ('2', '4')
                then 1
                else 0
            end as is_negro
        from {{ ref("sisbolsas_tb_dado_pessoal") }} as dp
    ),

    agregacao as (
        select
            ub.unidade,
            count(distinct ub.co_usuario) as total_bolsistas,
            count(
                distinct case when d.is_mulher = 1 then ub.co_usuario end
            ) as total_mulheres,
            count(
                distinct case when d.is_negro = 1 then ub.co_usuario end
            ) as total_negros
        from unidade_por_bolsista as ub
        left join demografia as d on ub.co_usuario = d.co_usuario
        group by 1
    ),

    metricas as (
        select
            unidade,
            total_bolsistas,
            total_mulheres,
            total_negros,
            ceil(total_bolsistas * 0.40)::integer as meta_minima_mulheres_40,
            ceil(total_bolsistas * 0.30)::integer as meta_minima_negros_30,
            round(
                (total_mulheres::numeric / nullif(total_bolsistas, 0)) * 100, 2
            ) as percentual_mulheres,
            round(
                (total_negros::numeric / nullif(total_bolsistas, 0)) * 100, 2
            ) as percentual_negros
        from agregacao
    )

select
    unidade,
    total_bolsistas,
    total_mulheres,
    total_negros,
    percentual_mulheres,
    percentual_negros,
    meta_minima_mulheres_40,
    meta_minima_negros_30,
    greatest(meta_minima_mulheres_40 - total_mulheres, 0) as mulheres_faltantes_meta,
    greatest(meta_minima_negros_30 - total_negros, 0) as negros_faltantes_meta
from metricas
order by total_bolsistas desc, unidade asc
