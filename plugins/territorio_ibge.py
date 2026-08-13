"""Regra territorial do IBGE para os planos de acao (secao 7.1 da
especificacao do banco do MinC).

O problema que este modulo resolve: **a API do Transferegov informa o codigo
do municipio da capital quando o ente recebedor e um estado**. Gravar esse
codigo como se fosse municipal faz o plano estadual de Sao Paulo virar um
plano do municipio de Sao Paulo -- e e exatamente isso que a validacao 12.7
do documento manda impedir.

A saida e sempre um dicionario com os quatro campos territoriais:

===================  ============  ===============  =====================
Tipo do ente         ``cod_ibge``  ``cod_ibge_uf``  ``cod_ibge_municipio``
===================  ============  ===============  =====================
Estado de Sao Paulo  ``35``        ``35``           ``None``
Municipio de SP      ``3550308``   ``35``           ``3550308``
===================  ============  ===============  =====================

Python puro (sem Airflow, sem banco) para continuar testavel fora do
container -- ver ``tests/test_territorio_ibge.py``.
"""

import re
import unicodedata
from typing import Any

# Codigo IBGE da UF por sigla. Usado como fallback quando o plano nao traz
# o codigo do municipio: sem ele, um plano estadual sem codigo ficaria sem
# nenhuma identificacao territorial.
SIGLA_PARA_CODIGO_UF: dict[str, str] = {
    "RO": "11",
    "AC": "12",
    "AM": "13",
    "RR": "14",
    "PA": "15",
    "AP": "16",
    "TO": "17",
    "MA": "21",
    "PI": "22",
    "CE": "23",
    "RN": "24",
    "PB": "25",
    "PE": "26",
    "AL": "27",
    "SE": "28",
    "BA": "29",
    "MG": "31",
    "ES": "32",
    "RJ": "33",
    "SP": "35",
    "PR": "41",
    "SC": "42",
    "RS": "43",
    "MS": "50",
    "MT": "51",
    "GO": "52",
    "DF": "53",
}

TIPO_ENTE_ESTADO = "ESTADO"
TIPO_ENTE_MUNICIPIO = "MUNICIPIO"

# "ESTADO DE SAO PAULO", "GOVERNO DO ESTADO DA BAHIA" -- ancorado no inicio
# de proposito: um "SECRETARIA DE ESTADO DE CULTURA DE X" e ente municipal e
# nao pode cair aqui so por conter a palavra "ESTADO".
_RE_ESTADO = re.compile(r"^(?:GOVERNO\s+D[OA]\s+)?ESTADO\b")


def _sem_acento(texto: Any) -> str:
    """Normaliza para comparacao: maiusculo, sem acento, espacos colapsados."""
    if texto is None:
        return ""
    s = unicodedata.normalize("NFKD", str(texto).strip().upper())
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", s).strip()


def _digitos(valor: Any) -> str:
    """Extrai so os digitos de um codigo IBGE.

    A API devolve o codigo ora como int, ora como string, e o
    ``json_normalize`` do ``ClientPostgresDB`` pode transformar em float
    (``3550308.0``) -- descartar tudo que nao e digito cobre os tres casos.
    """
    if valor is None:
        return ""
    return re.sub(r"\D", "", str(valor).split(".")[0])


def classificar_tipo_ente(nome_ente: Any) -> str:
    """Classifica o ente recebedor em ``ESTADO`` ou ``MUNICIPIO`` pelo nome.

    O Distrito Federal e tratado como estado: nao tem municipios, e o ente
    recebedor e o governo distrital. E o caso separado que o documento pede
    para tratar a parte -- ele nunca casaria com o padrao ``ESTADO DE ...``.
    """
    nome = _sem_acento(nome_ente)

    if "DISTRITO FEDERAL" in nome:
        return TIPO_ENTE_ESTADO

    return TIPO_ENTE_ESTADO if _RE_ESTADO.match(nome) else TIPO_ENTE_MUNICIPIO


def derivar_territorio(plano: dict[str, Any]) -> dict[str, str | None]:
    """Deriva ``tipo_ente``, ``cod_ibge``, ``cod_ibge_uf`` e
    ``cod_ibge_municipio`` a partir de um registro de ``/plano_acao``.

    Le ``nome_ente_recebedor_plano_acao``,
    ``codigo_ibge_municipio_ente_recebedor_plano_acao`` e
    ``uf_ente_recebedor_plano_acao``; nenhum e obrigatorio -- campo ausente
    vira ``None`` no lugar de excecao, porque a camada raw nao pode derrubar
    a carga inteira por um ente mal cadastrado na origem.
    """
    tipo_ente = classificar_tipo_ente(plano.get("nome_ente_recebedor_plano_acao"))

    codigo = _digitos(plano.get("codigo_ibge_municipio_ente_recebedor_plano_acao"))
    codigo_uf_por_sigla = SIGLA_PARA_CODIGO_UF.get(
        _sem_acento(plano.get("uf_ente_recebedor_plano_acao"))
    )

    if tipo_ente == TIPO_ENTE_ESTADO:
        # Aqui esta o ponto do documento: o codigo que veio e da capital, nao
        # do ente. So os dois primeiros digitos (a UF) descrevem o ente.
        cod_ibge_uf = codigo.zfill(7)[:2] if codigo else codigo_uf_por_sigla
        return {
            "tipo_ente": tipo_ente,
            "cod_ibge": cod_ibge_uf,
            "cod_ibge_uf": cod_ibge_uf,
            "cod_ibge_municipio": None,
        }

    if not codigo:
        # Municipio sem codigo na origem: da para saber a UF, nao da para
        # inventar o municipio.
        return {
            "tipo_ente": tipo_ente,
            "cod_ibge": None,
            "cod_ibge_uf": codigo_uf_por_sigla,
            "cod_ibge_municipio": None,
        }

    cod_municipio = codigo.zfill(7)
    return {
        "tipo_ente": tipo_ente,
        "cod_ibge": cod_municipio,
        "cod_ibge_uf": cod_municipio[:2],
        "cod_ibge_municipio": cod_municipio,
    }
