import logging
from datetime import date

import pytest

from dados_funcionais_handler import DadosFuncionaisHandler


def _registro(
    matricula: str,
    ingresso: str | None,
    exclusao: str | None = None,
    cod_exclusao: str | None = None,
):
    return {
        "matriculaSiape": matricula,
        "nomeSitFuncional": "ATIVO PERMANENTE",
        "dataIngressoFuncao": ingresso,
        "dataOcorrExclusao": exclusao,
        "codOcorrExclusao": cod_exclusao,
    }


@pytest.mark.parametrize(
    "valor, esperado",
    [
        ("01102026", date(2026, 10, 1)),
        (" 26092020 ", date(2020, 9, 26)),
        (None, None),
        ("", None),
        ("31022026", None),
        ("2026-10-01", None),
    ],
)
def test_parse_data_siape(valor, esperado) -> None:
    assert DadosFuncionaisHandler._parse_data_siape(valor) == esperado


def test_multiplos_ativos_escolhe_data_mais_recente_pelo_ano() -> None:
    # Como texto, "26092020" > "01102026"; como data, 2026 é mais recente.
    antigo = _registro("111", "26092020")
    recente = _registro("222", "01102026")

    escolhido = DadosFuncionaisHandler.select_best_registro([antigo, recente])

    assert escolhido["matriculaSiape"] == "222"


def test_multiplos_ativos_ignora_data_invalida() -> None:
    invalido = _registro("111", "99999999")
    valido = _registro("222", "01012020")

    escolhido = DadosFuncionaisHandler.select_best_registro([invalido, valido])

    assert escolhido["matriculaSiape"] == "222"


def test_prefere_registro_ativo_a_registro_excluido_mais_recente() -> None:
    excluido = _registro("111", "01102026", exclusao="05102026")
    ativo = _registro("222", "01012000")

    escolhido = DadosFuncionaisHandler.select_best_registro([excluido, ativo])

    assert escolhido["matriculaSiape"] == "222"


def test_sem_ativos_escolhe_excluido_mais_recente_pelo_ano() -> None:
    antigo = _registro("111", "26092020", exclusao="01012021")
    recente = _registro("222", "01102026", exclusao="05102026")

    escolhido = DadosFuncionaisHandler.select_best_registro([antigo, recente])

    assert escolhido["matriculaSiape"] == "222"


def test_sem_ativos_e_sem_data_de_funcao_escolhe_exclusao_mais_recente() -> None:
    # Caso real: nenhum vínculo tem dataIngressoFuncao e o mais antigo vem primeiro.
    cedido_2019 = _registro("111", None, exclusao="07082019", cod_exclusao="02108")
    duplicidade = _registro("222", None, exclusao="08082019", cod_exclusao="02027")
    descentralizado_2026 = _registro(
        "333", None, exclusao="01102026", cod_exclusao="02253"
    )

    escolhido = DadosFuncionaisHandler.select_best_registro(
        [cedido_2019, duplicidade, descentralizado_2026]
    )

    assert escolhido["matriculaSiape"] == "333"


def test_sem_ativos_ignora_exclusao_por_erro_de_cadastramento() -> None:
    valido = _registro("111", None, exclusao="01012020", cod_exclusao="02108")
    duplicidade = _registro("222", None, exclusao="01012025", cod_exclusao="02027")

    escolhido = DadosFuncionaisHandler.select_best_registro([valido, duplicidade])

    assert escolhido["matriculaSiape"] == "111"


def test_sem_ativos_apenas_erros_de_cadastramento_escolhe_o_mais_recente() -> None:
    antigo = _registro("111", None, exclusao="01012020", cod_exclusao="02027")
    recente = _registro("222", None, exclusao="01012025", cod_exclusao="02027")

    escolhido = DadosFuncionaisHandler.select_best_registro([antigo, recente])

    assert escolhido["matriculaSiape"] == "222"


def test_sem_ativos_exclusao_invalida_fica_por_ultimo() -> None:
    invalido = _registro("111", None, exclusao="99999999")
    valido = _registro("222", None, exclusao="01012020")

    escolhido = DadosFuncionaisHandler.select_best_registro([invalido, valido])

    assert escolhido["matriculaSiape"] == "222"


def test_registro_unico_retornado_sem_alteracao() -> None:
    registro = _registro("111", None)

    assert DadosFuncionaisHandler.select_best_registro([registro]) is registro


def test_lista_vazia_retorna_dict_vazio() -> None:
    assert DadosFuncionaisHandler.select_best_registro([]) == {}


def test_loga_todos_os_vinculos_quando_ha_mais_de_um(caplog) -> None:
    registros = [_registro("111", "26092020"), _registro("222", "01102026")]

    with caplog.at_level(logging.INFO):
        DadosFuncionaisHandler.select_best_registro(registros)

    assert "SIAPE devolveu 2 vínculos para o mesmo CPF" in caplog.text
    assert "'matriculaSiape': '111'" in caplog.text
    assert "'matriculaSiape': '222'" in caplog.text


def test_nao_loga_vinculos_quando_ha_apenas_um(caplog) -> None:
    with caplog.at_level(logging.INFO):
        DadosFuncionaisHandler.select_best_registro([_registro("111", "01102026")])

    assert "vínculos para o mesmo CPF" not in caplog.text
