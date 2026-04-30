select distinct
    df.cod_atividade_funcao,
    df.cod_funcao,
    df.cod_jornada,
    df.cod_ocorr_ingresso_orgao,
    df.cod_ocorr_ingresso_serv_publico,
    df.cod_orgao,
    df.cod_padrao,
    df.cod_situacao_funcional,
    df.cod_uorg_exercicio,
    df.cod_upag,
    df.cod_orgao_origem,
    df.cpf_chefia_imediata,
    df.dt_exercicio_no_orgao,
    df.dt_fim_vale_ar,
    df.dt_ingresso_funcao,
    df.dt_ocorr_ingresso_orgao,
    df.dt_ocorr_ingresso_serv_publico,
    df.email_chefia_imediata,
    df.email_institucional,
    df.email_servidor,
    df.ident_unica,
    df.matricula_siape,
    df.modalidade_pgd,
    df.nome_atividade_funcao,
    df.nome_chefe_uorg,
    df.nome_funcao,
    df.nome_jornada,
    df.nome_ocorr_ingresso_orgao,
    df.nome_ocorr_ingresso_serv_publico,
    df.nome_orgao,
    df.nome_regime_juridico,
    df.nome_situacao_funcional,
    df.nome_uorg_exercicio,
    df.nome_upag,
    df.participa_pgd,
    df.percentual_ts,
    df.sigla_orgao,
    df.sigla_orgao_origem,
    df.sigla_regime_juridico,
    df.sigla_uorg_exercicio,
    df.sigla_upag,
    df.cpf,
    df.cod_cargo,
    df.cod_classe,
    df.cod_ocorr_aposentadoria,
    df.dt_ini_vale_ar,
    df.dt_ocorr_aposentadoria,
    df.nome_cargo,
    df.nome_classe,
    df.nome_ocorr_aposentadoria,
    df.sigla_nivel_cargo,
    df.tipo_vale_ar,
    df.cod_ocorr_isencao_ir,
    df.dt_ini_ocorr_isencao_ir,
    df.nome_ocorr_isencao_ir,
    df.cod_uorg_lotacao,
    df.nome_uorg_lotacao,
    df.sigla_uorg_lotacao,
    df.dt_fim_ocorr_isencao_ir,
    df.cod_ocorr_exclusao,
    df.dt_ocorr_exclusao,
    df.nome_ocorr_exclusao,
    df.dt_uorg_lotacao,
    df.cod_vale_transporte,
    df.valor_vale_transporte,
    df.dt_uorg_exercicio,
    df.pontuacao_desempenho,
    case
        when df.modalidade_pgd is null
        then 'Não participa'
        when df.modalidade_pgd = 'parcial'
        then 'Parcial'
        when df.modalidade_pgd = 'integral'
        then 'Integral'
        when df.modalidade_pgd = 'presencial'
        then 'Presencial'
        when df.modalidade_pgd = 'no exterior'
        then 'No exterior'
    end as pdg,
    case
        when df.nome_situacao_funcional = 'ATIVO EM OUTRO ORGAO'
        then 'Ativo em outro órgão'
        else df.sigla_uorg_exercicio
    end as unidade_exercicio,
    du.nome_municipio_uorg,
    greatest(df.dt_ingest, du.dt_ingest) as dt_ingest
from {{ ref("dados_funcionais") }} as df
inner join {{ ref("dados_uorg") }} as du on df.sigla_uorg_exercicio = du.sigla_uorg
