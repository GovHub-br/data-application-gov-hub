with
    dados_escolares_raw as (
        select
            codcurso,
            nomecurso,
            codmatricula,
            codorgao,
            codtitulacao,
            nometitulacao,
            codescolaridade,
            nomeescolaridade,
            cpf
        from {{ source("siape", "dados_escolares") }}
    )

select
    nullif(trim(codcurso), '') as cod_curso,
    nullif(trim(nomecurso), '') as nome_curso,
    nullif(trim(codmatricula), '') as cod_matricula,
    nullif(trim(codorgao), '') as cod_orgao,
    nullif(trim(codtitulacao), '') as cod_titulacao,
    nullif(trim(nometitulacao), '') as nome_titulacao,
    nullif(trim(codescolaridade), '') as cod_escolaridade,
    nullif(trim(nomeescolaridade), '') as nome_escolaridade,
    regexp_replace(nullif(trim(cpf), ''), '[^0-9]', '', 'g') as cpf
from dados_escolares_raw
