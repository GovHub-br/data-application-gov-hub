/*
    Anexo III - Relacao de Pagamentos (IPEA / APES) a partir do Sisbolsas.
    Grao: 1 linha por despesa lancada (sisbolsas_tb_lancamento_despesa).

    Traducao do relatorio SQL Server (arquivos/sql/anexo_iii_relatorio_sqlserver.sql)
    para Postgres/dbt. Os campos que so existem na foto do comprovante ou no
    extrato bancario (favorecido, forma de pagamento, conciliacao de extrato)
    entram como null, mantendo o nome da coluna para preenchimento manual.
*/

with
    lancamentos as (
        select
            co_lancamento_despesa,
            co_usuario,
            co_selecao,
            nu_bolsa,
            co_tipo_despesa,
            case
                when st_lancamento_despesa ~ '^[0-9]+$'
                then st_lancamento_despesa::integer
            end as st_lancamento_despesa,
            vl_despesa,
            vl_aprovado,
            vl_glossado,
            tp_comprovante,
            nullif(btrim(ds_titulo_comprovante), '') as ds_titulo_comprovante,
            nullif(btrim(tx_lancamento_despesa), '') as tx_lancamento_despesa,
            ds_numero_nota,
            case
                when dt_comprovante ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(dt_comprovante from 1 for 10)::date
            end as dt_comprovante
        from {{ ref("sisbolsas_tb_lancamento_despesa") }}
    ),

    base as (
        select
            l.*,
            row_number() over (
                partition by l.co_usuario, l.co_selecao, l.nu_bolsa
                order by l.dt_comprovante nulls last, l.co_lancamento_despesa::integer
            ) as seq
        from lancamentos as l
    ),

    /*
        Natureza (C/K) e Categoria derivadas de TB_TIPO_DESPESA. tp_categoria = 1
        corresponde a Capital (Equipamentos/Materiais Permanentes); os demais
        (tp_categoria = 2) sao Custeio. O codigo curto vem do texto normalizado
        (sem acentos) para tolerar novos tipos de despesa.
    */
    tipos_despesa as (
        select
            co_tipo_despesa,
            tp_categoria,
            ds_tipo_despesa,
            case when tp_categoria = '1' then 'K' else 'C' end as natureza,
            case
                when ds_norm like '%diaria%' then 'DIA'
                when ds_norm like '%transp%' then 'TRA'
                when ds_norm like '%fisica%' then 'SPF'
                when ds_norm like '%juridica%' then 'SPJ'
                when ds_norm like '%material de consumo%' then 'MAT'
                when ds_norm like '%equipam%'
                    or ds_norm like '%permanent%'
                    or ds_norm like '%bem%'
                    or ds_norm like '%bens%'
                then 'BES'
            end as categoria
        from (
            select
                co_tipo_despesa,
                tp_categoria,
                ds_tipo_despesa,
                translate(
                    lower(ds_tipo_despesa),
                    'áàâãäéèêëíìîïóòôõöúùûüç',
                    'aaaaaeeeeiiiiooooouuuuc'
                ) as ds_norm
            from {{ ref("sisbolsas_tb_tipo_despesa") }}
        ) as t
    ),

    usuarios as (
        select co_usuario, ds_nome, ds_email
        from {{ ref("sisbolsas_tb_usuario") }}
    ),

    -- 1 registro pessoal (CPF) por usuario, o de menor co_dado_pessoal
    dado_pessoal_por_usuario as (
        select distinct on (co_usuario)
            co_usuario,
            co_dado_pessoal,
            ds_cpf
        from {{ ref("sisbolsas_tb_dado_pessoal") }}
        order by co_usuario, co_dado_pessoal::integer
    ),

    -- Cabecalho do auxilio: nº SEI e data da ordem bancaria do credito
    concessao as (
        select
            co_usuario,
            co_selecao,
            nu_bolsa,
            ds_numero_sei,
            case
                when dt_ordem_bancaria ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(dt_ordem_bancaria from 1 for 10)::date
            end as dt_ordem_bancaria
        from {{ ref("sisbolsas_tb_concessao_auxilio") }}
    ),

    -- Conta bancaria preferindo a ativa (instrumento financeiro)
    instrumento_bancario as (
        select distinct on (db.co_dado_pessoal)
            db.co_dado_pessoal,
            inst.ds_nome
            || ' - Ag. ' || db.ds_numero_agencia
            || ' - C/C ' || db.ds_numero_conta as instrumento
        from {{ ref("sisbolsas_tb_dado_bancario") }} as db
        left join
            {{ ref("sisbolsas_tb_instituicao_financeira") }} as inst
            on inst.co_instituicao_financeira = db.co_instituicao_financeira
        order by db.co_dado_pessoal, db.in_ativo desc nulls last, db.co_dado_bancario::integer desc
    ),

    -- Anexos do lancamento (contagem + lista concatenada)
    anexos as (
        select
            la.co_lancamento_despesa,
            count(*) as qtd_anexos,
            string_agg(
                coalesce(nullif(btrim(a.ds_original), ''), a.ds_anexo)
                || case
                    when ta.ds_tipo_anexo is not null
                    then ' (' || ta.ds_tipo_anexo || ')'
                    else ''
                end,
                ' | '
                order by a.co_anexo::integer
            ) as anexos
        from {{ ref("sisbolsas_tb_lancadespe_anexo") }} as la
        join {{ ref("sisbolsas_tb_anexo") }} as a on a.co_anexo = la.co_anexo
        left join
            {{ ref("sisbolsas_tb_tipo_anexo") }} as ta
            on ta.co_tipo_anexo = a.co_tipo_anexo
        group by la.co_lancamento_despesa
    ),

    -- Historico de observacoes concatenado por data
    historico as (
        select
            co_lancamento_despesa,
            case
                when dt_criacao ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(dt_criacao from 1 for 10)::date
            end as dt_criacao,
            tx_observacao
        from {{ ref("sisbolsas_tb_hist_lanca_despesa") }}
    ),

    observacoes as (
        select
            co_lancamento_despesa,
            string_agg(
                coalesce(to_char(dt_criacao, 'DD/MM/YYYY') || ': ', '')
                || tx_observacao,
                ' | '
                order by dt_criacao nulls last
            ) as observacoes
        from historico
        group by co_lancamento_despesa
    )

select
    -- Cabecalho (repetido por linha)
    u.ds_nome as beneficiario_auxilio,
    dp.ds_cpf as cpf,
    ca.ds_numero_sei as numero_auxilio_sei,
    ib.instrumento as instrumento_financeiro,

    -- Linha da despesa (Anexo III)
    b.seq,
    case b.st_lancamento_despesa
        when 1 then 'Em análise'
        when 2 then 'Devolvida (ajuste solicitado)'
        when 4 then 'Deferida / Aprovada'
        when 5 then 'Indeferida / Glosada'
        else 'Código ' || b.st_lancamento_despesa::text
    end as situacao,
    b.st_lancamento_despesa as situacao_codigo,
    td.natureza,
    td.categoria as codigo_categoria,
    lpad(b.seq::text, 3, '0')
        || '-' || coalesce(td.natureza, '?')
        || '-' || coalesce(td.categoria, '???') as id_despesa,
    cast(null as text) as favorecido,
    cast(null as text) as cpf_cnpj_favorecido,
    b.ds_numero_nota as num_doc_fiscal,
    coalesce(
        b.ds_titulo_comprovante,
        b.tx_lancamento_despesa,
        td.ds_tipo_despesa
    ) as descricao_objeto,
    b.dt_comprovante as data_despesa,
    b.dt_comprovante as data_documento,
    b.vl_despesa as valor_documento,
    cast(null as text) as forma_pagamento,
    cast(null as text) as correspondencia_extrato,
    cast(null as text) as tipo_transacao,
    cast(null as date) as data_lancamento_extrato,
    obs.observacoes as observacao_justificativa,

    -- Extras uteis (fora do template)
    b.vl_aprovado as valor_aprovado,
    b.vl_glossado as valor_glossado,
    td.ds_tipo_despesa as categoria_descricao,
    b.tp_comprovante as tipo_comprovante_cod,
    ca.dt_ordem_bancaria as data_ordem_bancaria_auxilio,
    coalesce(ax.qtd_anexos, 0) as qtd_anexos,
    ax.anexos as anexos_arquivos,
    b.co_lancamento_despesa,
    b.co_usuario,
    b.co_selecao,
    b.nu_bolsa
from base as b
left join tipos_despesa as td on b.co_tipo_despesa = td.co_tipo_despesa
left join usuarios as u on b.co_usuario = u.co_usuario
left join dado_pessoal_por_usuario as dp on b.co_usuario = dp.co_usuario
left join
    concessao as ca
    on b.co_usuario = ca.co_usuario
    and b.co_selecao = ca.co_selecao
    and b.nu_bolsa = ca.nu_bolsa
left join instrumento_bancario as ib on dp.co_dado_pessoal = ib.co_dado_pessoal
left join anexos as ax on b.co_lancamento_despesa = ax.co_lancamento_despesa
left join observacoes as obs on b.co_lancamento_despesa = obs.co_lancamento_despesa
order by b.co_usuario, b.co_selecao, b.nu_bolsa, b.seq
