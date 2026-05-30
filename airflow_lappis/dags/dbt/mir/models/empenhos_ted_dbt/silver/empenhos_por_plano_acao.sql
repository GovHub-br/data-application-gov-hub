with
    empenhos_sem_vinculo_ted as (
        select
            *,
            right(ne_ccor, 12) as ne,
            left(ne_ccor, 6) as orgao_id,
            null as nc,
            null as num_transf,
            'sem vinculo' as metodo
        from {{ ref("empenhos_tesouro_ted") }}
        where
            ne_ccor_descricao ~* '\bTED[[:space:]:/().-]*(S/?[VN]|S/?VINCULO)'
            or ne_ccor_descricao ~* 'SEM[[:space:]]+VINC[[:space:]]*(ULO|/TED)'
    ),
    empenhos_filtrados as (
        select *
        from {{ ref("empenhos_tesouro_ted") }}
        where
            ne_ccor_descricao !~* '\bTED[[:space:]:/().-]*(S/?[VN]|S/?VINCULO)'
            and ne_ccor_descricao !~* 'SEM[[:space:]]+VINC[[:space:]]*(ULO|/TED)'

    ),
    empenhos_orgaos_metodo_1 as (
        select
            *,
            -- Uma série de extrações que servirão de identificadores 
            right(ne_ccor, 12) as ne,
            left(ne_ccor, 6) as orgao_id,
            {{ target.schema }}.format_nc(
                regexp_substr(ne_ccor_descricao, '([0-9]{4}NC[0-9]+)')
            ) as nc,
            replace(
                (
                    regexp_match(
                        ne_ccor_descricao,
                        '(FERENCIA|TED|CRICAO|TRANSF.|TRANF.|TRANF. |TRANSFERENCIA |TRANSFERENCIA:)(\s|^|-|)([0-9]{6}|1\w{5}|[0-9]{3}\.[0-9]{3})(\s|$|\.|,|-|\/)',
                        'i'
                    )
                )[3],
                '.',
                ''
            ) as num_transf,
            'metodo 1' as metodo
        from empenhos_filtrados
    ),

    empenhos_restantes_metodo_1 as (
        select * from empenhos_orgaos_metodo_1 where num_transf is null and nc is null
    ),

    empenhos_orgaos_metodo_2 as (
        select
            programa_governo,
            programa_governo_descricao,
            acao_governo,
            acao_governo_descricao,
            emissao_mes,
            emissao_dia,
            ne_ccor,
            ne_num_processo,
            ne_info_complementar,
            ne_ccor_descricao,
            doc_observacao,
            natureza_despesa,
            natureza_despesa_descricao,
            ne_ccor_favorecido,
            ne_ccor_favorecido_descricao,
            ne_ccor_ano_emissao,
            ptres,
            fonte_recursos_detalhada,
            fonte_recursos_detalhada_descricao,
            despesas_empenhadas,
            despesas_liquidadas,
            despesas_pagas,
            restos_a_pagar_inscritos,
            restos_a_pagar_pagos,
            dt_ingest,
            ne,
            orgao_id,
            nc,
            replace(
                (
                    regexp_match(
                        ne_ccor_descricao,
                        '.*(?:NOTA DE (TRANSFERENCIA|TRANFERENCIA|CREDITO))[:.[:space:]-]*((?=[A-Za-z0-9]*[0-9])[A-Za-z0-9]{6,})',
                        'i'
                    )
                )[2],
                '.',
                ''
            ) as num_transf,
            'metodo 2' as metodo
        from empenhos_restantes_metodo_1 p
    ),

    empenhos_restantes_metodo_2 as (
        select * from empenhos_orgaos_metodo_2 where num_transf is null and nc is null
    ),

    empenhos_orgaos_metodo_3 as (
        select
            programa_governo,
            programa_governo_descricao,
            acao_governo,
            acao_governo_descricao,
            emissao_mes,
            emissao_dia,
            ne_ccor,
            ne_num_processo,
            ne_info_complementar,
            ne_ccor_descricao,
            doc_observacao,
            natureza_despesa,
            natureza_despesa_descricao,
            ne_ccor_favorecido,
            ne_ccor_favorecido_descricao,
            ne_ccor_ano_emissao,
            ptres,
            fonte_recursos_detalhada,
            fonte_recursos_detalhada_descricao,
            despesas_empenhadas,
            despesas_liquidadas,
            despesas_pagas,
            restos_a_pagar_inscritos,
            restos_a_pagar_pagos,
            dt_ingest,
            ne,
            orgao_id,
            nc,
            replace(
                (
                    regexp_match(
                        ne_ccor_descricao,
                        '.*(?:(?:TED(?:[[:space:]]*[-.N∞øº°∅()]*))[[:space:]]*|(?:SIAFI[[:space:]]+N∫))[[:space:].-]*(?<![0-9])(([0-9]{6})|(1[A-Za-z0-9]{5}))(?![0-9])',
                        'i'
                    )
                )[1],
                '.',
                ''
            ) as num_transf,
            'metodo 3' as metodo
        from empenhos_restantes_metodo_2 p
    ),

    empenhos_restantes_metodo_3 as (
        select * from empenhos_orgaos_metodo_3 where num_transf is null and nc is null
    ),

    empenhos_orgaos_metodo_4 as (
        select
            programa_governo,
            programa_governo_descricao,
            acao_governo,
            acao_governo_descricao,
            emissao_mes,
            emissao_dia,
            ne_ccor,
            ne_num_processo,
            ne_info_complementar,
            ne_ccor_descricao,
            doc_observacao,
            natureza_despesa,
            natureza_despesa_descricao,
            ne_ccor_favorecido,
            ne_ccor_favorecido_descricao,
            ne_ccor_ano_emissao,
            ptres,
            fonte_recursos_detalhada,
            fonte_recursos_detalhada_descricao,
            despesas_empenhadas,
            despesas_liquidadas,
            despesas_pagas,
            restos_a_pagar_inscritos,
            restos_a_pagar_pagos,
            dt_ingest,
            ne,
            orgao_id,
            nc,
            replace(
                (
                    regexp_match(
                        fonte_recursos_detalhada_descricao,
                        'TED(?::)?(?:[[:space:]]+[A-Z/]+)?[[:space:]:-]*N?[∞∫ºo]?[[:space:]]*[0-9/]*[[:space:]:;,-]*[ø-]?[[:space:]]*([0-9]{6}|1[A-Z0-9]{5})',
                        'i'
                    )
                )[1],
                '.',
                ''
            ) as num_transf,
            'metodo 4' as metodo
        from empenhos_restantes_metodo_3 p
    ),

    empenhos_restantes_metodo_4 as (
        select * from empenhos_orgaos_metodo_4 where num_transf is null and nc is null
    ),

    empenhos_orgaos_metodo_5 as (
        select
            programa_governo,
            programa_governo_descricao,
            acao_governo,
            acao_governo_descricao,
            emissao_mes,
            emissao_dia,
            ne_ccor,
            ne_num_processo,
            ne_info_complementar,
            ne_ccor_descricao,
            doc_observacao,
            natureza_despesa,
            natureza_despesa_descricao,
            ne_ccor_favorecido,
            ne_ccor_favorecido_descricao,
            ne_ccor_ano_emissao,
            ptres,
            fonte_recursos_detalhada,
            fonte_recursos_detalhada_descricao,
            despesas_empenhadas,
            despesas_liquidadas,
            despesas_pagas,
            restos_a_pagar_inscritos,
            restos_a_pagar_pagos,
            dt_ingest,
            ne,
            orgao_id,
            nc,
            replace(
                (regexp_match(ne_info_complementar, '^([0-9]{6}|1[A-Z0-9]{5})$', 'i'))[1],
                '.',
                ''
            ) as num_transf,
            'metodo 5' as metodo
        from empenhos_restantes_metodo_4
    ),

    empenhos_restantes_metodo_5 as (
        select * from empenhos_orgaos_metodo_5 where num_transf is null and nc is null
    ),

    empenhos_teds_invalidos as (
        select
            programa_governo,
            programa_governo_descricao,
            acao_governo,
            acao_governo_descricao,
            emissao_mes,
            emissao_dia,
            ne_ccor,
            ne_num_processo,
            ne_info_complementar,
            ne_ccor_descricao,
            doc_observacao,
            natureza_despesa,
            natureza_despesa_descricao,
            ne_ccor_favorecido,
            ne_ccor_favorecido_descricao,
            ne_ccor_ano_emissao,
            ptres,
            fonte_recursos_detalhada,
            fonte_recursos_detalhada_descricao,
            despesas_empenhadas,
            despesas_liquidadas,
            despesas_pagas,
            restos_a_pagar_inscritos,
            restos_a_pagar_pagos,
            dt_ingest,
            ne,
            orgao_id,
            regexp_substr(
                ne_ccor_descricao,
                '((?<![0-9])[0-9]{0,3}NC[0-9]+|[0-9]{5,}NC[0-9]+|[0-9]{4}NC(?![0-9]))'
            ) as nc,
            null as num_transf,
            'ted ou nc invalido' as metodo
        from empenhos_restantes_metodo_5
    ),

    empenhos_restantes_teds_invalidos as (
        select
            programa_governo,
            programa_governo_descricao,
            acao_governo,
            acao_governo_descricao,
            emissao_mes,
            emissao_dia,
            ne_ccor,
            ne_num_processo,
            ne_info_complementar,
            ne_ccor_descricao,
            doc_observacao,
            natureza_despesa,
            natureza_despesa_descricao,
            ne_ccor_favorecido,
            ne_ccor_favorecido_descricao,
            ne_ccor_ano_emissao,
            ptres,
            fonte_recursos_detalhada,
            fonte_recursos_detalhada_descricao,
            despesas_empenhadas,
            despesas_liquidadas,
            despesas_pagas,
            restos_a_pagar_inscritos,
            restos_a_pagar_pagos,
            dt_ingest,
            ne,
            orgao_id,
            nc,
            num_transf,
            'vinculo nao encontrado' as metodo
        from empenhos_teds_invalidos
        where num_transf is null and nc is null
    ),

    raw_union as (
        select *
        from empenhos_sem_vinculo_ted
        union all
        select *
        from empenhos_orgaos_metodo_1
        where num_transf is not null or nc is not null
        union all
        select *
        from empenhos_orgaos_metodo_2
        where num_transf is not null or nc is not null
        union all
        select *
        from empenhos_orgaos_metodo_3
        where num_transf is not null or nc is not null
        union all
        select *
        from empenhos_orgaos_metodo_4
        where num_transf is not null or nc is not null
        union all
        select *
        from empenhos_orgaos_metodo_5
        where num_transf is not null or nc is not null
        union all
        select *
        from empenhos_teds_invalidos
        where num_transf is not null or nc is not null
        union all
        select *
        from empenhos_restantes_teds_invalidos
    ),

    ids_agregados_nc_ccor as (
        select ne_ccor, max(nc) as nc, max(num_transf) as num_transf
        from raw_union
        group by ne_ccor
    ),

    empenhos_orgaos_metodo_6 as (
        select
            ert.emissao_mes,
            ert.emissao_dia,
            ert.ne_ccor,
            ert.ne_num_processo,
            ert.ne_info_complementar,
            ert.ne_ccor_descricao,
            ert.doc_observacao,
            ert.natureza_despesa,
            ert.natureza_despesa_descricao,
            ert.ne_ccor_favorecido,
            ert.ne_ccor_favorecido_descricao,
            ert.ne_ccor_ano_emissao,
            ert.ptres,
            ert.fonte_recursos_detalhada,
            ert.fonte_recursos_detalhada_descricao,
            ert.despesas_empenhadas,
            ert.despesas_liquidadas,
            ert.despesas_pagas,
            ert.restos_a_pagar_inscritos,
            ert.restos_a_pagar_pagos,
            ert.dt_ingest,
            ert.ne,
            ert.orgao_id,
            coalesce(ert.nc, r.nc) as nc,
            coalesce(ert.num_transf, r.num_transf) as num_transf,
            -- método calculado dinamicamente
            case
                when
                    (ert.nc is null and r.nc is not null)
                    or (ert.num_transf is null and r.num_transf is not null)
                then 'metodo 6'
                else ert.metodo
            end as metodo
        from raw_union ert
        left join ids_agregados_nc_ccor r using (ne_ccor)
    ),

    base_empenhos_orgaos_metodo_7 as (
        select
            -- seleciona todas as colunas do órgãos 1, exceto nc e num_transf
            *,
            trim(
                both ' -'
                from
                    regexp_replace(
                        (
                            regexp_match(
                                ne_ccor_descricao,
                                'TED [[:space:].:NR∫º°-]*(?:([A-Za-zÀ-ÿ/][A-Za-zÀ-ÿ0-9/ \\-]*)[[:space:]\\-]+)?([0-9]{1,5}(?:[./ \\-][0-9]{2,4})?)',
                                'i'
                            )
                        )[1],
                        '\s+',
                        ' ',
                        'g'
                    )
            ) as complemento_ted,
            replace(
                (
                    regexp_match(
                        ne_ccor_descricao,
                        'TED [[:space:].:NR∫º°-]*(?:([A-Za-zÀ-ÿ/][A-Za-zÀ-ÿ0-9/ \\-]*)[[:space:]\\-]+)?([0-9]{1,5}(?:[./ \\-][0-9]{2,4})?)',
                        'i'
                    )
                )[2],
                '.',
                ''
            ) as num_ted,
            'metodo 1' as metodo_ted
        from empenhos_orgaos_metodo_6
    ),

    base_metodo_7 as (
        select
            *,
            (regexp_match(num_ted, '^([0-9]{1,5})(?:[/.\- ]([0-9]{2,4}))?$'))[
                1
            ] as numero_base,
            (regexp_match(num_ted, '^([0-9]{1,5})(?:[/.\- ]([0-9]{2,4}))?$'))[
                2
            ] as ano_raw
        from base_empenhos_orgaos_metodo_7
    ),
    norm_metodo_7 as (
        select
            *,
            case
                when ano_raw is null
                then null
                when length(ano_raw) = 2
                then
                    case
                        when ano_raw::int <= 30
                        then '20' || ano_raw  -- 24 → 2024
                        else '19' || ano_raw  -- 95 → 1995
                    end
                else ano_raw
            end as ano_normalizado
        from base_metodo_7
    ),
    agrupado_metodo_7 as (
        -- calculamos o ano oficial APENAS para numero_base "longos"
        select orgao_id, numero_base, max(ano_normalizado) as ano_oficial
        from norm_metodo_7
        where length(numero_base) >= 3 and ano_normalizado is not null
        group by orgao_id, numero_base
    ),

    empenhos_orgaos_metodo_7 as (
        select
            a.*,
            g.ano_oficial,
            case
                when length(a.numero_base) <= 2 and a.ano_normalizado is null
                then null

                -- se o registro não tem ano, e o numero_base é "longo", e há um ano
                -- oficial no grupo -> preencher
                when
                    a.ano_normalizado is null
                    and length(a.numero_base) >= 3
                    and g.ano_oficial is not null
                then a.numero_base || '/' || g.ano_oficial

                -- se o registro já tem ano_normalizado -> manter esse ano (normalizado)
                when a.ano_normalizado is not null
                then a.numero_base || '/' || a.ano_normalizado

                -- caso contrário (nenhum ano encontrado) -> deixar só o numero_base
                else a.numero_base
            end as numero_ted_normalizado
        from norm_metodo_7 a
        left join
            agrupado_metodo_7 g
            on a.orgao_id = g.orgao_id
            and a.numero_base = g.numero_base
    ),

    empenhos_restantes_metodo_7 as (
        select * from empenhos_orgaos_metodo_7 where numero_ted_normalizado is null
    ),

    base_empenhos_orgaos_metodo_8 as (
        select
            -- seleciona todas as colunas do órgãos 1, exceto nc e num_transf
            emissao_mes,
            emissao_dia,
            ne_ccor,
            ne_num_processo,
            ne_info_complementar,
            ne_ccor_descricao,
            doc_observacao,
            natureza_despesa,
            natureza_despesa_descricao,
            ne_ccor_favorecido,
            ne_ccor_favorecido_descricao,
            ne_ccor_ano_emissao,
            ptres,
            fonte_recursos_detalhada,
            fonte_recursos_detalhada_descricao,
            despesas_empenhadas,
            despesas_liquidadas,
            despesas_pagas,
            restos_a_pagar_inscritos,
            restos_a_pagar_pagos,
            dt_ingest,
            ne,
            orgao_id,
            nc,
            num_transf,
            metodo,
            trim(
                both ' -'
                from
                    regexp_replace(
                        (
                            regexp_match(
                                doc_observacao,
                                'TED [[:space:].:NR∫º°-]*(?:([A-Za-zÀ-ÿ/][A-Za-zÀ-ÿ0-9/ \\-]*)[[:space:]\\-]+)?([0-9]{1,5}(?:[./ \\-][0-9]{2,4})?)',
                                'i'
                            )
                        )[1],
                        '\s+',
                        ' ',
                        'g'
                    )
            ) as complemento_ted,
            replace(
                (
                    regexp_match(
                        doc_observacao,
                        'TED [[:space:].:NR∫º°-]*(?:([A-Za-zÀ-ÿ/][A-Za-zÀ-ÿ0-9/ \\-]*)[[:space:]\\-]+)?([0-9]{1,5}(?:[./ \\-][0-9]{2,4})?)',
                        'i'
                    )
                )[2],
                '.',
                ''
            ) as num_ted,
            'metodo 2' as metodo_ted
        from empenhos_restantes_metodo_7
    ),

    base_metodo_8 as (
        select
            *,
            (regexp_match(num_ted, '^([0-9]{1,5})(?:[/.\- ]([0-9]{2,4}))?$'))[
                1
            ] as numero_base,
            (regexp_match(num_ted, '^([0-9]{1,5})(?:[/.\- ]([0-9]{2,4}))?$'))[
                2
            ] as ano_raw
        from base_empenhos_orgaos_metodo_8
    ),
    norm_metodo_8 as (
        select
            *,
            case
                when ano_raw is null
                then null
                when length(ano_raw) = 2
                then
                    case
                        when ano_raw::int <= 30
                        then '20' || ano_raw  -- 24 → 2024
                        else '19' || ano_raw  -- 95 → 1995
                    end
                else ano_raw
            end as ano_normalizado
        from base_metodo_8
    ),
    agrupado_metodo_8 as (
        -- calculamos o ano oficial APENAS para numero_base "longos"
        select orgao_id, numero_base, max(ano_normalizado) as ano_oficial
        from norm_metodo_7
        where length(numero_base) >= 3 and ano_normalizado is not null
        group by orgao_id, numero_base
    ),
    empenhos_orgaos_metodo_8 as (
        select
            a.*,
            g.ano_oficial,
            case
                when length(a.numero_base) <= 2 and a.ano_normalizado is null
                then null

                -- se o registro não tem ano, e o numero_base é "longo", e há um ano
                -- oficial no grupo -> preencher
                when
                    a.ano_normalizado is null
                    and length(a.numero_base) >= 3
                    and g.ano_oficial is not null
                then a.numero_base || '/' || g.ano_oficial

                -- se o registro já tem ano_normalizado -> manter esse ano (normalizado)
                when a.ano_normalizado is not null
                then a.numero_base || '/' || a.ano_normalizado

                -- caso contrário (nenhum ano encontrado) -> deixar só o numero_base
                else a.numero_base
            end as numero_ted_normalizado
        from norm_metodo_8 a
        left join
            agrupado_metodo_8 g
            on a.orgao_id = g.orgao_id
            and a.numero_base = g.numero_base
    ),
    empenhos_restantes_metodo_8 as (
        select * from empenhos_orgaos_metodo_8 where numero_ted_normalizado is null
    ),

    union_metodo_7_8 as (
        select *
        from empenhos_orgaos_metodo_7
        where numero_ted_normalizado is not null
        union all
        select *
        from empenhos_orgaos_metodo_8
        where numero_ted_normalizado is not null
        union all
        select *
        from empenhos_restantes_metodo_8
    ),

    ids_agregados_num_ted_normalizado as (
        select
            orgao_id, numero_ted_normalizado, max(nc) as nc, max(num_transf) as num_transf
        from union_metodo_7_8
        group by orgao_id, numero_ted_normalizado
    ),

    empenhos_orgaos_metodo_9 as (
        select
            ert.emissao_mes,
            ert.emissao_dia,
            ert.ne_ccor,
            ert.ne_num_processo,
            ert.ne_info_complementar,
            ert.ne_ccor_descricao,
            ert.doc_observacao,
            ert.natureza_despesa,
            ert.natureza_despesa_descricao,
            ert.ne_ccor_favorecido,
            ert.ne_ccor_favorecido_descricao,
            ert.ne_ccor_ano_emissao,
            ert.ptres,
            ert.fonte_recursos_detalhada,
            ert.fonte_recursos_detalhada_descricao,
            ert.despesas_empenhadas,
            ert.despesas_liquidadas,
            ert.despesas_pagas,
            ert.restos_a_pagar_inscritos,
            ert.restos_a_pagar_pagos,
            ert.dt_ingest,
            ert.ne,
            ert.orgao_id,
            coalesce(ert.nc, r.nc) as nc,
            coalesce(ert.num_transf, r.num_transf) as num_transf,
            -- método calculado dinamicamente
            case
                when
                    (ert.nc is null and r.nc is not null)
                    or (ert.num_transf is null and r.num_transf is not null)
                then 'metodo 9'
                else ert.metodo
            end as metodo,
            complemento_ted,
            num_ted,
            metodo_ted,
            numero_base,
            ano_raw,
            ano_normalizado,
            ano_oficial,
            numero_ted_normalizado
        from union_metodo_7_8 ert
        left join
            ids_agregados_num_ted_normalizado r using (orgao_id, numero_ted_normalizado)
    ),
    empenhos_restantes_metodo_9 as (
        select *
        from empenhos_orgaos_metodo_9
        where (nc != '') or (num_transf is not null)
    ),
    planos_de_acao as (
        select * from {{ ref("num_transf_n_plano_acao") }} where plano_acao is not null
    ),
    result_table as (
        select distinct
            er.*, pa.plano_acao::integer as plano_acao, pa.num_transf as num_transf_pa
        from empenhos_restantes_metodo_9 er
        left join planos_de_acao pa on er.num_transf = cast(pa.num_transf as text)
    )  --

select *
from result_table
