with

    -- =============================================
    -- Empenhos do SIAFI já correlacionados com contratos
    -- (via Silver contratos_empenhos, que faz matching em 4 etapas:
    --  NE+CNPJ, processo, CNPJ único, info complementar)
    -- =============================================
    empenhos_siafi as (
        select
            ce.contrato_id::int,
            ce.ne_transformed as nota_empenho,
            ce.ne_ccor,
            ce.ne_num_processo,
            ce.ne_ccor_descricao,
            ce.doc_observacao,
            ce.natureza_despesa,
            ce.natureza_despesa_descricao,
            ce.ne_ccor_favorecido as cnpj_cpf_favorecido,
            ce.ne_ccor_ano_emissao as ano_emissao,
            ce.despesas_empenhadas,
            ce.despesas_liquidadas,
            ce.despesas_pagas,
            ce.fornecedor_tipo,
            ce.fornecedor_nome,
            ce.fornecedor_cnpj_cpf_idgener,
            ce.numero as numero_contrato,
            ce.unidades_requisitantes,
            ce.objeto,
            ce.vigencia_inicio,
            ce.vigencia_fim,
            ce.dt_ingest
        from {{ ref('contratos_empenhos') }} ce
        -- exclui linhas de contratos sem empenhos (vindas do full join do Silver)
        where ce.ne_transformed is not null
    ),

    -- =============================================
    -- Empenhos do ComprasGov (vínculo direto com contrato)
    -- =============================================
    empenhos_comprasgov as (
        select
            e.contrato_id::int as contrato_id,
            upper(e.nota_empenho) as nota_empenho,
            regexp_replace(e.credor_obj_cnpj_cpf_idgener, '[/.-]', '', 'g') as cnpj_cpf_favorecido,
            e.credor_obj_nome as favorecido_nome,
            e.naturezadespesa as natureza_despesa,
            e.empenhado as despesas_empenhadas,
            e.liquidado as despesas_liquidadas,
            e.pago as despesas_pagas,
            e.data_emissao,
            e.dt_ingest
        from {{ ref('empenhos') }} e
        where e.contrato_id is not null
    ),

    -- =============================================
    -- Todos os empenhos SIAFI (para encontrar não correlacionados)
    -- =============================================
    siafi_todos as (
        select
            upper(right(ne_ccor, 12)) as nota_empenho,
            ne_ccor,
            ne_num_processo,
            ne_ccor_descricao,
            doc_observacao,
            natureza_despesa,
            natureza_despesa_descricao,
            ne_ccor_favorecido as cnpj_cpf_favorecido,
            ne_ccor_favorecido_descricao as favorecido_nome,
            ne_ccor_ano_emissao as ano_emissao,
            despesas_empenhadas,
            despesas_liquidadas,
            despesas_pagas,
            dt_ingest
        from {{ ref('empenhos_tesouro') }}
    ),

    -- =============================================
    -- Dados dos contratos para enriquecer ComprasGov-only
    -- =============================================
    contratos_base as (
        select
            id::int as contrato_id,
            fornecedor_tipo,
            fornecedor_nome,
            fornecedor_cnpj_cpf_idgener,
            numero as numero_contrato,
            unidades_requisitantes,
            objeto,
            vigencia_inicio,
            vigencia_fim,
            dt_ingest as dt_ingest_contrato
        from {{ ref('contratos') }}
    ),

    -- =============================================
    -- Cruzamento: SIAFI correlacionados x ComprasGov
    -- Se a NE existe nos 2, mostra apenas 1 (prioriza SIAFI como fonte oficial)
    -- =============================================
    cruzamento as (
        select
            coalesce(s.contrato_id, cg.contrato_id) as contrato_id,
            coalesce(s.nota_empenho, cg.nota_empenho) as nota_empenho,
            s.ne_ccor,
            s.ne_num_processo,
            s.ne_ccor_descricao,
            s.doc_observacao,
            coalesce(s.natureza_despesa, cg.natureza_despesa) as natureza_despesa,
            s.natureza_despesa_descricao,
            coalesce(s.cnpj_cpf_favorecido, cg.cnpj_cpf_favorecido) as cnpj_cpf_favorecido,
            s.ano_emissao,
            -- Prioriza valores do SIAFI (fonte oficial)
            coalesce(s.despesas_empenhadas, cg.despesas_empenhadas) as despesas_empenhadas,
            coalesce(s.despesas_liquidadas, cg.despesas_liquidadas) as despesas_liquidadas,
            coalesce(s.despesas_pagas, cg.despesas_pagas) as despesas_pagas,
            s.fornecedor_tipo,
            s.fornecedor_nome,
            s.fornecedor_cnpj_cpf_idgener,
            s.numero_contrato,
            s.unidades_requisitantes,
            s.objeto,
            s.vigencia_inicio,
            s.vigencia_fim,
            case
                when s.nota_empenho is not null and cg.nota_empenho is not null then 'Ambos'
                when s.nota_empenho is not null then 'SIAFI'
                else 'ComprasGov'
            end as origem_dados,
            false as sem_vinculo_contrato,
            greatest(s.dt_ingest, cg.dt_ingest) as dt_ingest
        from empenhos_siafi s
        full outer join empenhos_comprasgov cg
            on s.nota_empenho = cg.nota_empenho
    ),

    -- Enriquecer linhas ComprasGov-only com dados do contrato
    cruzamento_enriquecido as (
        select
            cr.contrato_id,
            cr.nota_empenho,
            cr.ne_ccor,
            cr.ne_num_processo,
            cr.ne_ccor_descricao,
            cr.doc_observacao,
            cr.natureza_despesa,
            cr.natureza_despesa_descricao,
            cr.cnpj_cpf_favorecido,
            cr.ano_emissao,
            cr.despesas_empenhadas,
            cr.despesas_liquidadas,
            cr.despesas_pagas,
            coalesce(cr.fornecedor_tipo, cb.fornecedor_tipo) as fornecedor_tipo,
            coalesce(cr.fornecedor_nome, cb.fornecedor_nome) as fornecedor_nome,
            coalesce(cr.fornecedor_cnpj_cpf_idgener, cb.fornecedor_cnpj_cpf_idgener) as fornecedor_cnpj_cpf_idgener,
            coalesce(cr.numero_contrato, cb.numero_contrato) as numero_contrato,
            coalesce(cr.unidades_requisitantes, cb.unidades_requisitantes) as unidades_requisitantes,
            coalesce(cr.objeto, cb.objeto) as objeto,
            coalesce(cr.vigencia_inicio, cb.vigencia_inicio) as vigencia_inicio,
            coalesce(cr.vigencia_fim, cb.vigencia_fim) as vigencia_fim,
            cr.origem_dados,
            cr.sem_vinculo_contrato,
            greatest(cr.dt_ingest, cb.dt_ingest_contrato) as dt_ingest
        from cruzamento cr
        left join contratos_base cb on cr.contrato_id = cb.contrato_id
    ),

    -- =============================================
    -- Empenhos SIAFI que NÃO foram correlacionados a nenhum contrato
    -- (indica erro de preenchimento / processo interno a melhorar)
    -- =============================================
    siafi_sem_contrato as (
        select
            null::int as contrato_id,
            st.nota_empenho,
            st.ne_ccor,
            st.ne_num_processo,
            st.ne_ccor_descricao,
            st.doc_observacao,
            st.natureza_despesa,
            st.natureza_despesa_descricao,
            st.cnpj_cpf_favorecido,
            st.ano_emissao,
            st.despesas_empenhadas,
            st.despesas_liquidadas,
            st.despesas_pagas,
            null::text as fornecedor_tipo,
            null::text as fornecedor_nome,
            null::text as fornecedor_cnpj_cpf_idgener,
            null::text as numero_contrato,
            null::text as unidades_requisitantes,
            null::text as objeto,
            null::date as vigencia_inicio,
            null::date as vigencia_fim,
            'SIAFI' as origem_dados,
            true as sem_vinculo_contrato,
            st.dt_ingest
        from siafi_todos st
        where st.ne_ccor not in (
            select ne_ccor from empenhos_siafi where ne_ccor is not null
        )
    )

-- União final: cruzamento + SIAFI sem contrato (erro de preenchimento)
select * from cruzamento_enriquecido
union all
select * from siafi_sem_contrato
