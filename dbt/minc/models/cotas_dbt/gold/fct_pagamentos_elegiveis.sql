-- Base do denominador: 1 linha por pagamento (payment-first / LEFT JOIN).
-- Preserva TODOS os pagamentos; expõe órfãos (tem_perfil=false), inclui sem_ano.
with pagamentos as (
    select * from {{ ref('contemplados_unif') }}
),
perfil as (
    select * from {{ ref('perfil_agentes_normalizado') }}
),
territorio as (
    select * from {{ ref('territorio_municipio') }}
)
select
    p.identificador_unico,
    p.valor_pago_num,
    p.ano_final,
    p.origem_ano,
    p.origem,
    p.chave_anonimizada,
    p.nome_edital,
    p.nome_programa,
    p.anexo_id,
    p.nome_arquivo,
    (pf.identificador_unico is not null)   as tem_perfil,
    coalesce(pf.flag_negra, false)         as flag_negra,
    coalesce(pf.flag_indigena, false)      as flag_indigena,
    coalesce(pf.is_pcd, false)             as flag_pcd,
    -- Território vulnerabilizado (LPG, cota 20%): o crosswalk territorio_municipio
    -- vem da tabela IBGE de FCU = territórios PERIFÉRICOS. A regra (definição do
    -- edital LPG, confirmada pela usuária) é POR MUNICÍPIO: município que aparece
    -- no crosswalk (tem área periférica) => território vulnerabilizado. Casamento
    -- por NOME cidade+uf (agente não tem código IBGE).
    pf.cidade                              as agente_cidade,
    pf.uf                                  as agente_uf,
    (terr.chave_municipio_uf is not null)  as tem_territorio,
    -- flag_territorio_vulneravel: casou no crosswalk periférico => vulnerabilizado.
    -- Sem cidade => NULL (não classificável). NOTA: casamento por nome ~47,5% dos
    -- com-perfil; refinar grafias depois. Meta da cota = 20% (ver distribuicao_cotas_agentes).
    case
        when pf.cidade is null or btrim(pf.cidade) = '' then null
        else (terr.chave_municipio_uf is not null)
    end                                    as flag_territorio_vulneravel
from pagamentos p
left join perfil pf
    on p.identificador_unico = pf.identificador_unico
   and p.identificador_unico is not null
left join territorio terr
    on pf.chave_municipio_uf = terr.chave_municipio_uf
