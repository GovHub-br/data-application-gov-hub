-- Território a nível de MUNICÍPIO (a partir do crosswalk de setores IBGE CD2022).
-- A fonte territorio_fcu_setores é grão setor censitário; aqui colapsa p/ município
-- e classifica se o município é (ou não) concentração urbana FCU.
--
-- CHAVE DE JUNÇÃO com os agentes = nome do município + UF normalizados (sem_acento
-- + lower + trim), porque os contemplados/agentes LPG NÃO têm código IBGE, só
-- cidade/uf em texto. Junção por nome é imperfeita (grafias divergentes), por isso
-- o modelo expõe a chave normalizada p/ auditar o casamento.
with setores as (
    select
        cd_mun,
        nm_mun,
        cd_uf,
        nm_uf,
        cd_fcu,
        nm_fcu
    from {{ source('transferegov_fundo_a_fundo', 'territorio_fcu_setores') }}
),
por_municipio as (
    select
        cd_mun,
        max(nm_mun)                                     as nm_mun,
        max(cd_uf)                                      as cd_uf,
        max(nm_uf)                                      as nm_uf,
        -- um município está numa concentração urbana se tem ao menos um setor
        -- com FCU preenchida (cd_fcu não nulo/vazio)
        bool_or(cd_fcu is not null and btrim(cd_fcu) <> '') as em_concentracao_urbana
    from setores
    group by cd_mun
),
-- mapa UF: código IBGE (2 díg) -> sigla. Necessário porque o crosswalk traz
-- nm_uf por extenso ("Rondônia") mas os agentes LPG podem gravar a UF como
-- SIGLA ("RO"). Gerar a chave por SIGLA garante o casamento no formato mais
-- comum; cd_uf (código) mapeia p/ sigla de forma determinística.
uf_sigla as (
    select * from (values
        ('11','RO'),('12','AC'),('13','AM'),('14','RR'),('15','PA'),('16','AP'),('17','TO'),
        ('21','MA'),('22','PI'),('23','CE'),('24','RN'),('25','PB'),('26','PE'),('27','AL'),('28','SE'),('29','BA'),
        ('31','MG'),('32','ES'),('33','RJ'),('35','SP'),
        ('41','PR'),('42','SC'),('43','RS'),
        ('50','MS'),('51','MT'),('52','GO'),('53','DF')
    ) as m(cd_uf, sigla_uf)
)
select
    pm.cd_mun,
    pm.nm_mun,
    pm.cd_uf,
    pm.nm_uf,
    us.sigla_uf,
    pm.em_concentracao_urbana,
    -- chave por SIGLA (formato mais provável do agente LPG): "municipio|uf"
    {{ sem_acento('pm.nm_mun') }} || '|' || lower(us.sigla_uf) as chave_municipio_uf
from por_municipio pm
left join uf_sigla us on pm.cd_uf = us.cd_uf
