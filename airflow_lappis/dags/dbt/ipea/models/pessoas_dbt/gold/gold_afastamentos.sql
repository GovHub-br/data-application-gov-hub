select
    cpf,
    gr_matricula as matricula,
    nome_pessoa,
    cod_funcao,
    sigla_uorg_exercicio,
    ano_exercicio,
    dt_ini as dt_inicio_afastamento,
    dt_fim as dt_fim_afastamento,
    qtde_dias,
    cod_ocorrencia,
    desc_ocorrencia,
    cod_diploma_afastamento,
    desc_diploma_afastamento,
    numero_diploma_afastamento,
    dt_publicacao_afastamento,
    origem_dados,
    dt_ingest
from {{ ref('afastamento_consolidado') }}
