-- cargos siape + siorg
-- ver logica para achar os cargos vagos e mudar essa tabela !
-- como o siorg lista todos os cargos, os que vierem null no siape provavelmente estão
-- vagos
select
    siorg.codigounidade as siorg_cod_unidade,
    siorg.nomeunidade as siorg_nome_unidade,
    siorg.siglaunidade as siorg_sigla_unidade,
    siorg.municipio as siorg_municipio_unidade,
    siorg.uf as siorg_uf_unidade,
    siorg.denominacao as siorg_denominacao_cargo,
    siorg.funcao as siorg_funcao,
    siorg.codigo_instancia as siorg_cod_instancia_cargo,
    siorg.cpf_titular as siorg_cpf_titular,
    siorg.nome_titular as siorg_nome_titular,

    siape.cpf as siape_cpf,
    siape.nome_pessoa as siape_nome_pessoa,
    siape.nome_cargo as siape_nome_cargo_efetivo,
    siape.nome_funcao as siape_nome_funcao_comissionada,
    siape.cod_uorg_exercicio as siape_cod_uorg,
    siape.nome_uorg_exercicio as siape_nome_uorg,
    siape.sigla_uorg_exercicio as siape_sigla_uorg,
    siape.uf_uorg as siape_uf_uorg,
    siape.nome_situacao_funcional as siape_situacao_funcional

from {{ ref("servidores_detalhados") }} siape
left join
    {{ ref("estrutura_organizacional_cargos") }} siorg on siape.cpf = siorg.cpf_titular  -- tabela siorg   
