with dados_funcionais_raw as (select * from {{ source("siape", "dados_funcionais") }})

select
    nullif(trim(codativfun), '') as cod_atividade_funcao,
    nullif(trim(codfuncao), '') as cod_funcao,
    nullif(trim(codjornada), '') as cod_jornada,
    nullif(trim(codocorringressoorgao), '') as cod_ocorr_ingresso_orgao,
    nullif(trim(codocorringressoservpublico), '') as cod_ocorr_ingresso_serv_publico,
    nullif(trim(codorgao), '') as cod_orgao,
    nullif(trim(codpadrao), '') as cod_padrao,
    nullif(trim(codsitfuncional), '') as cod_situacao_funcional,
    nullif(trim(coduorgexercicio), '') as cod_uorg_exercicio,
    nullif(trim(codupag), '') as cod_upag,
    nullif(trim(codigoorgaoorigem), '') as cod_orgao_origem,
    regexp_replace(
        nullif(trim(cpfchefiaimediata), ''), '[^0-9]', '', 'g'
    ) as cpf_chefia_imediata,
    to_date(nullif(trim(dataexercicionoorgao), ''), 'DDMMYYYY') as dt_exercicio_no_orgao,
    to_date(nullif(trim(datafimvalear), ''), 'DDMMYYYY') as dt_fim_vale_ar,
    to_date(nullif(trim(dataingressofuncao), ''), 'DDMMYYYY') as dt_ingresso_funcao,
    to_date(
        nullif(trim(dataocorringressoorgao), ''), 'DDMMYYYY'
    ) as dt_ocorr_ingresso_orgao,
    to_date(
        nullif(trim(dataocorringressoservpublico), ''), 'DDMMYYYY'
    ) as dt_ocorr_ingresso_serv_publico,
    lower(nullif(trim(emailchefiaimediata), '')) as email_chefia_imediata,
    lower(nullif(trim(emailinstitucional), '')) as email_institucional,
    lower(nullif(trim(emailservidor), '')) as email_servidor,
    nullif(trim(identunica), '') as ident_unica,
    nullif(trim(matriculasiape), '') as matricula_siape,
    nullif(trim(modalidadepgd), '') as modalidade_pgd,
    nullif(trim(nomeativfun), '') as nome_atividade_funcao,
    nullif(trim(nomechefeuorg), '') as nome_chefe_uorg,
    nullif(trim(nomefuncao), '') as nome_funcao,
    nullif(trim(nomejornada), '') as nome_jornada,
    nullif(trim(nomeocorringressoorgao), '') as nome_ocorr_ingresso_orgao,
    nullif(trim(nomeocorringressoservpublico), '') as nome_ocorr_ingresso_serv_publico,
    nullif(trim(nomeorgao), '') as nome_orgao,
    nullif(trim(nomeregimejuridico), '') as nome_regime_juridico,
    nullif(trim(nomesitfuncional), '') as nome_situacao_funcional,
    nullif(trim(nomeuorgexercicio), '') as nome_uorg_exercicio,
    nullif(trim(nomeupag), '') as nome_upag,
    nullif(trim(participapgd), '') as participa_pgd,
    cast(replace(nullif(trim(percentualts), ''), ',', '.') as numeric)
    / 100 as percentual_ts,
    nullif(trim(siglaorgao), '') as sigla_orgao,
    nullif(trim(siglaorgaoorigem), '') as sigla_orgao_origem,
    nullif(trim(siglaregimejuridico), '') as sigla_regime_juridico,
    nullif(trim(siglauorgexercicio), '') as sigla_uorg_exercicio,
    nullif(trim(siglaupag), '') as sigla_upag,
    regexp_replace(nullif(trim(cpf), ''), '[^0-9]', '', 'g') as cpf,
    nullif(trim(codcargo), '') as cod_cargo,
    nullif(trim(codclasse), '') as cod_classe,
    nullif(trim(codocorraposentadoria), '') as cod_ocorr_aposentadoria,
    to_date(nullif(trim(datainivalear), ''), 'DDMMYYYY') as dt_ini_vale_ar,
    to_date(
        nullif(trim(dataocorraposentadoria), ''), 'DDMMYYYY'
    ) as dt_ocorr_aposentadoria,
    nullif(trim(nomecargo), '') as nome_cargo,
    nullif(trim(nomeclasse), '') as nome_classe,
    nullif(trim(nomeocorraposentadoria), '') as nome_ocorr_aposentadoria,
    nullif(trim(siglanivelcargo), '') as sigla_nivel_cargo,
    nullif(trim(tipovalear), '') as tipo_vale_ar,
    nullif(trim(codocorrisencaoir), '') as cod_ocorr_isencao_ir,
    to_date(
        nullif(trim(datainiocorrisencaoir), ''), 'DDMMYYYY'
    ) as dt_ini_ocorr_isencao_ir,
    nullif(trim(nomeocorrisencaoir), '') as nome_ocorr_isencao_ir,
    nullif(trim(coduorglotacao), '') as cod_uorg_lotacao,
    nullif(trim(nomeuorglotacao), '') as nome_uorg_lotacao,
    nullif(trim(siglauorglotacao), '') as sigla_uorg_lotacao,
    to_date(
        nullif(trim(datafimocorrisencaoir), ''), 'DDMMYYYY'
    ) as dt_fim_ocorr_isencao_ir,
    nullif(trim(codocorrexclusao), '') as cod_ocorr_exclusao,
    to_date(nullif(trim(dataocorrexclusao), ''), 'DDMMYYYY') as dt_ocorr_exclusao,
    nullif(trim(nomeocorrexclusao), '') as nome_ocorr_exclusao,
    to_date(nullif(trim(datauorglotacao), ''), 'DDMMYYYY') as dt_uorg_lotacao,
    nullif(trim(codvaletransporte), '') as cod_vale_transporte,
    cast(
        replace(nullif(trim(valorvaletransporte), ''), ',', '.') as numeric
    ) as valor_vale_transporte,
    to_date(nullif(trim(datauorgexercicio), ''), 'DDMMYYYY') as dt_uorg_exercicio,
    nullif(trim(pontuacaodesempenho), '') as pontuacao_desempenho  -- Mantido como varchar (Não da pra saber se é "A","B" ou é um número > tudo null)
from dados_funcionais_raw
