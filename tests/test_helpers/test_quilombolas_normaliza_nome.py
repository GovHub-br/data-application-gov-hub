"""Testes unitários para a função `normalizar_nome_coluna`.

Combina técnicas Caixa-Preta (Particionamento de Equivalência e Valor Limite) e Caixa-Branca (Cobertura de Decisões/Branches e MC/DC).
"""

from __future__ import annotations

import pytest
import sys
from pathlib import Path

RAIZ_DO_PROJETO = Path(__file__).resolve().parents[1]
if str(RAIZ_DO_PROJETO) not in sys.path:
    sys.path.insert(0, str(RAIZ_DO_PROJETO))

from airflow_lappis.helpers.quilombolas_parser import normalizar_nome_coluna


@pytest.mark.parametrize(
    ("nome_original", "esperado"),
    [
        ("Nome do Cliente", "nome_do_cliente"),          # Particionamento de Equivalência: Espaços/Maiúsculas
        ("Data-de-Nascimento", "data_de_nascimento"),    # Particionamento de Equivalência: Hifens
        ("Ação", "acao"),                                # Particionamento de Equivalência: Acentos
        ("Desconto %", "desconto__porcentagem"),         # Particionamento de Equivalência: Símbolo %
        ("Valor (R$)", "valor_r"),                       # Particionamento de Equivalência: Caracteres especiais removidos
        ("   ", "___"),                                  # Particionamento de Equivalência: Espaços puros convertidos em underlines
    ],
)
def test_normaliza_classes_de_equivalencia_validas(nome_original: str, esperado: str) -> None:
    """Valida as regras de negócio de substituição de caracteres válidos."""
    assert normalizar_nome_coluna(nome_original, 1) == esperado


@pytest.mark.parametrize("nome_invalido", ["", "!@#$()", "???"])
def test_normaliza_fallback_strings_vazias_ou_simbolos(nome_invalido: str) -> None:
    """Valida o comportamento quando o input resulta em string vazia após a limpeza."""
    assert normalizar_nome_coluna(nome_invalido, 42) == "coluna_42"


def test_normaliza_fallback_string_literal_none() -> None:
    """Valida o particionamento de equivalência para o input literal "none"."""
    assert normalizar_nome_coluna("None", 99) == "coluna_99"


def test_normaliza_limite_maximo_exato_63_chars() -> None:
    """Testa a string na fronteira exata do limite de caracteres (63)."""
    nome_63 = "a" * 63
    resultado = normalizar_nome_coluna(nome_63, 1)
    
    assert len(resultado) == 63
    assert resultado == nome_63


def test_normaliza_truncamento_limite_64_chars() -> None:
    """Testa a string um caractere acima da fronteira (64)."""
    nome_64 = ("a" * 63) + "b"
    resultado = normalizar_nome_coluna(nome_64, 1)
    
    assert len(resultado) == 63
    assert resultado == ("a" * 63)
    assert "b" not in resultado