with base as (
    select
        diretoria_responsavel as diretoria,
        instrumento,
        objeto,
        entidades_externas,
        data_inicio as inicio,
        data_vencimento as conclusao,
        recursos_orcamentarios as recursos_externos,
        recursos_nao_orcamentarios as recursos_ipea,
        numero_do_proc as numero_do_processo,
        coordenador,
        nullif(
            trim(
                regexp_replace(
                    regexp_replace(
                        replace(
                            replace(fiscal_e_substituto, '&nbsp;', ' '),
                            '&#58;',
                            ':'
                        ),
                        '<[^>]*>',
                        ' ',
                        'g'
                    ),
                    '\s+',
                    ' ',
                    'g'
                )
            ),
            ''
        ) as fiscal_e_substituto,
        status as situacao
    from {{ ref("sgac_projetos_sgac") }}
)

select
    diretoria,
    instrumento,
    objeto,
    entidades_externas,
    inicio,
    conclusao,
    recursos_externos,
    recursos_ipea,
    coalesce(recursos_externos, 0) + coalesce(recursos_ipea, 0) as recursos_totais,
    numero_do_processo,
    coordenador,
    fiscal_e_substituto,
    situacao,
    conclusao - current_date as dias_para_vencer,
    date_trunc('month', conclusao)::date as mes_conclusao,
    extract(year from conclusao) as ano_conclusao,
    case
        when conclusao is null then 'Sem data de conclusão'
        when conclusao < current_date then 'Vencido'
        when conclusao <= current_date + interval '30 days'
        then 'Vence em até 30 dias'
        when conclusao <= current_date + interval '60 days'
        then 'Vence entre 31 e 60 dias'
        when conclusao <= current_date + interval '90 days'
        then 'Vence entre 61 e 90 dias'
        else 'Acima de 90 dias'
    end as faixa_vencimento,
    case
        when inicio <= current_date and conclusao >= current_date then 'Vigente'
        when conclusao < current_date then 'Encerrado/Vencido'
        when inicio > current_date then 'Ainda não iniciado'
        else 'Indefinido'
    end as status_vigencia,
    case
        when fiscal_e_substituto is null then 'Sem fiscal informado'
        else 'Com fiscal informado'
    end as status_fiscal,
    case
        when coordenador is null then 'Sem coordenador informado'
        else 'Com coordenador informado'
    end as status_coordenador
from base
order by conclusao, diretoria, instrumento, entidades_externas
