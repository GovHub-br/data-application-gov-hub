"""Testes da regra territorial do IBGE (secao 7.1 / validacao 12.7)."""

from territorio_ibge import classificar_tipo_ente, derivar_territorio


def _plano(nome: str, codigo: object = None, uf: str = "SP") -> dict[str, object]:
    return {
        "nome_ente_recebedor_plano_acao": nome,
        "codigo_ibge_municipio_ente_recebedor_plano_acao": codigo,
        "uf_ente_recebedor_plano_acao": uf,
    }


def test_estado_nao_herda_o_codigo_do_municipio_da_capital() -> None:
    # O caso que a validacao 12.7 manda impedir: a API devolve 3550308
    # (municipio de Sao Paulo) para o plano do ESTADO de Sao Paulo.
    territorio = derivar_territorio(_plano("ESTADO DE SÃO PAULO", 3550308))

    assert territorio == {
        "tipo_ente": "ESTADO",
        "cod_ibge": "35",
        "cod_ibge_uf": "35",
        "cod_ibge_municipio": None,
    }


def test_municipio_mantem_codigo_de_sete_digitos() -> None:
    territorio = derivar_territorio(_plano("MUNICÍPIO DE SÃO PAULO", 3550308))

    assert territorio == {
        "tipo_ente": "MUNICIPIO",
        "cod_ibge": "3550308",
        "cod_ibge_uf": "35",
        "cod_ibge_municipio": "3550308",
    }


def test_distrito_federal_e_tratado_como_estado() -> None:
    territorio = derivar_territorio(_plano("DISTRITO FEDERAL", 5300108, uf="DF"))

    assert territorio["tipo_ente"] == "ESTADO"
    assert territorio["cod_ibge"] == "53"
    assert territorio["cod_ibge_municipio"] is None


def test_codigo_com_zero_a_esquerda_nao_perde_digito() -> None:
    # Municipios de RO/AC comecam com "1" e o codigo pode chegar como int
    # curto; o zfill garante os 7 digitos.
    territorio = derivar_territorio(_plano("MUNICÍPIO DE GUAJARÁ-MIRIM", 110010, uf="RO"))

    assert territorio["cod_ibge"] == "0110010"


def test_codigo_como_float_do_json_normalize() -> None:
    territorio = derivar_territorio(_plano("MUNICÍPIO DE SÃO PAULO", "3550308.0"))

    assert territorio["cod_ibge_municipio"] == "3550308"


def test_municipio_sem_codigo_ainda_resolve_a_uf_pela_sigla() -> None:
    territorio = derivar_territorio(_plano("MUNICÍPIO DE SALVADOR", None, uf="BA"))

    assert territorio["cod_ibge"] is None
    assert territorio["cod_ibge_uf"] == "29"


def test_governo_do_estado_conta_como_estado() -> None:
    assert classificar_tipo_ente("GOVERNO DO ESTADO DA BAHIA") == "ESTADO"


def test_secretaria_de_estado_nao_vira_ente_estadual() -> None:
    # Conter a palavra "ESTADO" nao basta -- o padrao e ancorado no inicio.
    nome = "SECRETARIA DE ESTADO DE CULTURA DE PALMAS"
    assert classificar_tipo_ente(nome) == "MUNICIPIO"
