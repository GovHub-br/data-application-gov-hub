-- Lista de chaves distintas (para teste de unicidade e contagem secundária).
select distinct
    identificador_unico,
    tipo_proponente,
    origem
from {{ ref('perfil_agentes_normalizado') }}
