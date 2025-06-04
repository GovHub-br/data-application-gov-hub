with
    dados_curriculo_raw as (
        select
            cpf,
            identificunica,
            codigo,
            codcurso,
            nomecurso,
            dataconclusao,
            instituicao,
            nome,
            cargahoraria,
            cargo,
            datainicio,
            nomeorgaoempresa,
            datafim,
            projeto,
            infoadicionais,
            tipodesc
        from {{ source("siape", "dados_curriculo") }}
    )

select
    regexp_replace(nullif(trim(cpf), ''), '[^0-9]', '', 'g') as cpf,
    nullif(trim(identificunica), '') as ident_unica,
    nullif(trim(codigo), '') as codigo_experiencia,
    nullif(trim(codcurso), '') as cod_curso,
    nullif(trim(nomecurso), '') as nome_curso,
    -- Converte YYYYMM para DATE (1º dia do mês)
    to_date(nullif(trim(dataconclusao), '') || '01', 'YYYYMMDD') as dt_mes_conclusao,
    nullif(trim(instituicao), '') as nome_instituicao,
    nullif(trim(nome), '') as nome_area_experiencia,
    nullif(trim(cargahoraria), '') as carga_horaria,  -- Mantido como varchar para segurança
    nullif(trim(cargo), '') as nome_cargo,
    -- Converte YYYYMM para DATE (1º dia do mês)
    to_date(nullif(trim(datainicio), '') || '01', 'YYYYMMDD') as dt_mes_inicio,
    nullif(trim(nomeorgaoempresa), '') as nome_orgao_empresa,
    -- Converte YYYYMM para DATE (1º dia do mês)
    to_date(nullif(trim(datafim), '') || '01', 'YYYYMMDD') as dt_mes_fim,
    nullif(trim(projeto), '') as descricao_projeto,
    nullif(trim(infoadicionais), '') as informacoes_adicionais,
    nullif(trim(tipodesc), '') as tipo_descricao
from dados_curriculo_raw
