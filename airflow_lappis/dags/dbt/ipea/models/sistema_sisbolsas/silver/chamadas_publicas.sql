with
    chamadas_base as (
        select
            co_chamada_publica,
            co_projeto,
            co_situacao_chamada,
            co_usuario_criacao,
            co_programa,
            nu_chamada_publica,
            nu_ano,
            ds_chamada_publica,
            ds_numero_sei,
            vl_global_estimado,
            dt_ini_pesquisa,
            dt_fim_pesquisa,
            dt_publicacao_dou,
            dt_previsao_resultado,
            dt_ini_bolsa,
            dt_criacao,
            dt_inicio_inscricao,
            dt_fim_inscricao,
            dt_inicio_julgamento,
            dt_fim_julgamento,
            dt_publicacao_resultado,
            tp_moeda,
            dt_fim_recurso,
            dt_inicio_recurso,
            dt_inicio_previsao_bolsa
        from {{ ref("sisbolsas_tb_chamada_publica") }}
    ),

    chamadas as (
        select
            c.co_chamada_publica,
            regexp_replace(btrim(c.co_projeto, E' \t\n\r'), '[.]0+$', '') as co_projeto,
            c.co_situacao_chamada,
            c.co_usuario_criacao,
            c.co_programa,
            case
                when btrim(c.nu_chamada_publica, E' \t\n\r') ~ '^[0-9]+([.]0+)?$'
                then regexp_replace(btrim(c.nu_chamada_publica, E' \t\n\r'), '[.]0+$', '')
                else btrim(c.nu_chamada_publica, E' \t\n\r')
            end as nu_chamada_publica,
            case
                when btrim(c.nu_ano, E' \t\n\r') ~ '^[0-9]{4}([.]0+)?$'
                then regexp_replace(btrim(c.nu_ano, E' \t\n\r'), '[.]0+$', '')::integer
            end as ano_chamada,
            c.ds_chamada_publica,
            c.ds_numero_sei,
            c.vl_global_estimado,
            case
                when c.dt_ini_pesquisa ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(c.dt_ini_pesquisa from 1 for 10)::date
            end as dt_ini_pesquisa,
            case
                when c.dt_fim_pesquisa ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(c.dt_fim_pesquisa from 1 for 10)::date
            end as dt_fim_pesquisa,
            case
                when c.dt_publicacao_dou ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(c.dt_publicacao_dou from 1 for 10)::date
            end as dt_publicacao_dou,
            case
                when c.dt_previsao_resultado ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(c.dt_previsao_resultado from 1 for 10)::date
            end as dt_previsao_resultado,
            case
                when c.dt_ini_bolsa ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(c.dt_ini_bolsa from 1 for 10)::date
            end as dt_ini_bolsa,
            case
                when c.dt_criacao ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(c.dt_criacao from 1 for 10)::date
            end as dt_criacao,
            case
                when c.dt_inicio_inscricao ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(c.dt_inicio_inscricao from 1 for 10)::date
            end as dt_inicio_inscricao,
            case
                when c.dt_fim_inscricao ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(c.dt_fim_inscricao from 1 for 10)::date
            end as dt_fim_inscricao,
            case
                when c.dt_inicio_julgamento ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(c.dt_inicio_julgamento from 1 for 10)::date
            end as dt_inicio_julgamento,
            case
                when c.dt_fim_julgamento ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(c.dt_fim_julgamento from 1 for 10)::date
            end as dt_fim_julgamento,
            case
                when c.dt_publicacao_resultado ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(c.dt_publicacao_resultado from 1 for 10)::date
            end as dt_publicacao_resultado,
            case
                when c.dt_fim_recurso ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(c.dt_fim_recurso from 1 for 10)::date
            end as dt_fim_recurso,
            case
                when c.dt_inicio_recurso ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(c.dt_inicio_recurso from 1 for 10)::date
            end as dt_inicio_recurso,
            case
                when c.dt_inicio_previsao_bolsa ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(c.dt_inicio_previsao_bolsa from 1 for 10)::date
            end as dt_inicio_previsao_bolsa,
            c.tp_moeda,
            case
                when c.tp_moeda = '1'
                then 'Real (R$)'
                when c.tp_moeda is null
                then null
                else 'Estrangeira'
            end as moeda
        from chamadas_base as c
    ),

    usuarios as (
        select
            u.co_usuario,
            u.ds_nome,
            regexp_replace(u.ds_login, '[^0-9]', '', 'g') as ds_login_cpf
        from {{ ref("sisbolsas_tb_usuario") }} as u
    ),

    programas as (
        select p.co_programa, p.ds_programa
        from {{ ref("sisbolsas_tb_programa") }} as p
    ),

    situacoes as (
        select s.co_situacao_chamada, s.ds_situacao_chamada
        from {{ ref("sisbolsas_tb_situacao_chamada") }} as s
    ),

    unidades as (
        select
            cu.co_chamada_publica,
            string_agg(distinct u.ds_sigla, ' | ' order by u.ds_sigla) as unidades_sigla,
            string_agg(distinct u.ds_unidade, ' | ' order by u.ds_unidade) as unidades_nome,
            string_agg(distinct e.ds_uf, ' | ' order by e.ds_uf) as ufs
        from {{ ref("sisbolsas_tb_chapubli_unidade") }} as cu
        left join {{ ref("sisbolsas_tb_unidade") }} as u on cu.co_unidade = u.co_unidade
        left join {{ ref("sisbolsas_tb_estado") }} as e on u.co_estado = e.co_estado
        group by 1
    ),

    modalidades as (
        select
            s.co_chamada_publica,
            count(distinct s.co_selecao) as total_selecoes,
            string_agg(distinct m.ds_modalidade, ' | ' order by m.ds_modalidade)
            as modalidades,
            sum(
                case
                    when s.qt_bolsa ~ '^[0-9]+(\\.[0-9]+)?$'
                    then s.qt_bolsa::numeric
                end
            ) as cotas,
            sum(s.vl_total) as valor_total_selecoes,
            max(
                case
                    when s.qt_duracao ~ '^[0-9]+(\\.[0-9]+)?$'
                    then s.qt_duracao::numeric
                end
            ) as prazo_meses
        from {{ ref("sisbolsas_tb_selecao") }} as s
        left join {{ ref("sisbolsas_tb_modalidade") }} as m on s.co_modalidade = m.co_modalidade
        group by 1
    ),

    processos as (
        select
            p.co_selecao,
            p.in_classificacao,
            p.in_nao_apto,
            p.in_bolsa_ativa
        from {{ ref("sisbolsas_tb_processo_seletivo") }} as p
    ),

    processos_agg as (
        select
            s.co_chamada_publica,
            count(*) as total_processos,
            sum(case when p.in_classificacao is true then 1 else 0 end)
            as total_classificados,
            sum(case when p.in_nao_apto is true then 1 else 0 end) as total_nao_aptos,
            sum(case when p.in_bolsa_ativa is true then 1 else 0 end)
            as total_bolsas_ativas,
            sum(
                case
                    when p.in_classificacao is true
                    and not coalesce(p.in_bolsa_ativa, false)
                    then 1
                    else 0
                end
            ) as reservas
        from processos as p
        left join {{ ref("sisbolsas_tb_selecao") }} as s on p.co_selecao = s.co_selecao
        group by 1
    ),

    fontes_chamada as (
        select
            cf.co_chamada_publica,
            string_agg(
                distinct ff.ds_fonte_financeira,
                ' | '
                order by ff.ds_fonte_financeira
            ) as fontes_recurso_chamada
        from {{ ref("sisbolsas_tb_chapubli_fontfina") }} as cf
        left join
            {{ ref("sisbolsas_tb_fonte_financeira") }} as ff
            on cf.co_fonte_financeira = ff.co_fonte_financeira
        group by 1
    ),

    coordenadores_chamada as (
        select
            c.co_chamada_publica,
            string_agg(
                distinct coalesce(u.ds_nome, c.ds_cpf),
                ' | '
                order by coalesce(u.ds_nome, c.ds_cpf)
            ) as coordenador_chamada
        from {{ ref("sisbolsas_tb_coordenador") }} as c
        left join usuarios as u on c.ds_cpf = u.ds_login_cpf
        group by 1
    ),

    projetos as (
        select
            p.projetoid,
            p.projetoid::text as co_projeto,
            p.tituloprojeto,
            p.numeroprojeto,
            p.anoprojeto,
            p.coordenadorid,
            p.diretoriaid,
            p.statusprojetoid
        from {{ ref("ipea_pro_projetos") }} as p
    ),

    diretorias as (
        select d.diretoriaid, d.diretorianome, d.diretoriasigla
        from {{ ref("ipea_pro_diretorias") }} as d
    ),

    coordenadores_projeto as (
        select sp.servidorpublicoid, sp.nomeservidor
        from {{ ref("ipea_pro_servidorespublicos") }} as sp
    ),

    status_projetos as (
        select s.statusprojetoid, s.nomestatus
        from {{ ref("ipea_pro_statusprojetos") }} as s
    ),

    fontes_projeto as (
        select
            fr.projetoid::text as co_projeto,
            string_agg(
                distinct coalesce(ifr.nomeitemfontereceita, fr.descricaofonte),
                ' | '
                order by coalesce(ifr.nomeitemfontereceita, fr.descricaofonte)
            ) as fontes_recurso_projeto,
            sum(fr.valortotalfonte) as valor_total_fontes_projeto
        from {{ ref("ipea_pro_fontesreceitas") }} as fr
        left join
            {{ ref("ipea_pro_itemfontereceitas") }} as ifr
            on fr.itemfontereceitaid = ifr.itemfontereceitaid
        group by 1
    ),

    fontes_projeto_por_ano_raw as (
        select
            fr.projetoid,
            coalesce(
                extract(year from fr.datainiciofonte),
                extract(year from fr.datafinalfonte)
            )::integer as ano,
            sum(fr.valortotalfonte) as valor_ano
        from {{ ref("ipea_pro_fontesreceitas") }} as fr
        group by 1, 2
    ),

    fontes_projeto_por_ano as (
        select
            f.projetoid::text as co_projeto,
            jsonb_object_agg(f.ano, f.valor_ano order by f.ano)
            filter (where f.ano is not null) as valor_por_ano
        from fontes_projeto_por_ano_raw as f
        group by 1
    )

select
    c.co_chamada_publica as chamada_id,
    c.ds_numero_sei as processo_sei,
    concat_ws('/', c.nu_chamada_publica, c.ano_chamada::text) as numero_chamada,
    c.ano_chamada,
    concat_ws('/', c.nu_chamada_publica, c.ano_chamada::text) as numero_chamada_ano,
    c.ds_chamada_publica as descricao_chamada,
    prog.ds_programa as tipo_chamada,
    c.co_programa as programa_id,
    sit.ds_situacao_chamada as situacao_chamada,
    c.co_situacao_chamada as situacao_chamada_id,
    c.co_projeto as projeto_id,
    proj.tituloprojeto as projeto_titulo,
    proj.numeroprojeto as projeto_numero,
    proj.anoprojeto as projeto_ano,
    dir.diretorianome as diretoria,
    dir.diretoriasigla as diretoria_sigla,
    coalesce(coord_proj.nomeservidor, coord_chamada.coordenador_chamada)
    as coordenador,
    coord_chamada.coordenador_chamada,
    coord_proj.nomeservidor as coordenador_projeto,
    status_projeto.nomestatus as status_projeto,
    unidades.unidades_sigla,
    unidades.unidades_nome,
    unidades.ufs,
    modalidades.modalidades,
    modalidades.total_selecoes,
    modalidades.prazo_meses,
    c.dt_ini_pesquisa as inicio_pesquisa,
    c.dt_fim_pesquisa as fim_pesquisa,
    case
        when c.dt_ini_pesquisa is not null and c.dt_fim_pesquisa is not null
        then (c.dt_fim_pesquisa - c.dt_ini_pesquisa)
    end as prazo_pesquisa_dias,
    c.dt_inicio_inscricao as inicio_inscricao,
    c.dt_fim_inscricao as fim_inscricao,
    c.dt_inicio_julgamento as inicio_julgamento,
    c.dt_fim_julgamento as fim_julgamento,
    c.dt_publicacao_dou as publicacao_dou,
    c.dt_previsao_resultado as previsao_resultado,
    c.dt_publicacao_resultado as publicacao_resultado,
    c.dt_inicio_previsao_bolsa as inicio_previsao_bolsa,
    c.dt_ini_bolsa as inicio_bolsa,
    c.dt_inicio_recurso as inicio_recurso,
    c.dt_fim_recurso as fim_recurso,
    c.dt_criacao as data_criacao,
    c.moeda,
    coalesce(
        c.vl_global_estimado,
        modalidades.valor_total_selecoes,
        fontes_projeto.valor_total_fontes_projeto
    ) as valor_total_chamada,
    c.vl_global_estimado as valor_global_estimado,
    modalidades.valor_total_selecoes,
    fontes_projeto.valor_total_fontes_projeto,
    fontes_projeto_por_ano.valor_por_ano,
    coalesce(
        fontes_chamada.fontes_recurso_chamada,
        fontes_projeto.fontes_recurso_projeto
    ) as fonte_recurso,
    fontes_chamada.fontes_recurso_chamada,
    fontes_projeto.fontes_recurso_projeto,
    modalidades.cotas,
    processos.reservas,
    processos.total_processos,
    processos.total_classificados,
    processos.total_nao_aptos,
    processos.total_bolsas_ativas,
    usuario_criacao.ds_nome as usuario_criacao
from chamadas as c
left join programas as prog on c.co_programa = prog.co_programa
left join situacoes as sit on c.co_situacao_chamada = sit.co_situacao_chamada
left join unidades on c.co_chamada_publica = unidades.co_chamada_publica
left join modalidades on c.co_chamada_publica = modalidades.co_chamada_publica
left join processos_agg as processos on c.co_chamada_publica = processos.co_chamada_publica
left join fontes_chamada on c.co_chamada_publica = fontes_chamada.co_chamada_publica
left join coordenadores_chamada as coord_chamada
    on c.co_chamada_publica = coord_chamada.co_chamada_publica
left join projetos as proj on c.co_projeto = proj.co_projeto
left join diretorias as dir on proj.diretoriaid = dir.diretoriaid
left join coordenadores_projeto as coord_proj
    on proj.coordenadorid = coord_proj.servidorpublicoid
left join status_projetos as status_projeto
    on proj.statusprojetoid = status_projeto.statusprojetoid
left join fontes_projeto on c.co_projeto = fontes_projeto.co_projeto
left join fontes_projeto_por_ano on c.co_projeto = fontes_projeto_por_ano.co_projeto
left join usuarios as usuario_criacao on c.co_usuario_criacao = usuario_criacao.co_usuario
