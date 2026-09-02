select
    codigounidade,
    codigounidadepai,
    nome,
    sigla,
    codigoorgaoentidade,
    codigotipounidade,
    codigoesfera,
    codigopoder,
    codigonaturezajuridica,
    ordem_grandeza,
    caminho_unidade,
    dt_ingest
from {{ ref('unidade_organizacional') }}
