from unittest.mock import MagicMock, patch

import pytest

from cliente_postgres import ClientPostgresDB


def _mock_connect(columns: list[str], rows: list[tuple]) -> MagicMock:
    cursor = MagicMock()
    cursor.description = [(col,) for col in columns]
    cursor.fetchall.return_value = rows
    cursor.__enter__.return_value = cursor

    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


# ---------------------------------------------------------------------------
# get_id_* (ids lidos das tabelas de TED gravadas como texto)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "metodo, tabela, coluna",
    [
        ("get_id_metas", "planos_acao_meta", "id_meta"),
        ("get_id_notas_credito", "notas_de_credito", "id_nota"),
        ("get_id_programacoes_financeiras", "programacao_financeira", "id_programacao"),
    ],
)
def test_get_ids_converte_texto_e_descarta_vazios(
    metodo: str, tabela: str, coluna: str
) -> None:
    # a ingestao grava texto e o pandas pode transformar inteiro em '64.0'
    conn = _mock_connect([coluna], [("64.0",), ("3",), (None,), ("",), ("NaN",), (3,)])

    with patch("cliente_postgres.psycopg2.connect", return_value=conn):
        ids = getattr(ClientPostgresDB("dsn"), metodo)()

    assert ids == [3, 64]
    conn.cursor.return_value.execute.assert_called_once_with(
        f"SELECT DISTINCT {coluna} FROM transfere_gov.{tabela}"
    )


# ---------------------------------------------------------------------------
# _flatten_data
# ---------------------------------------------------------------------------
def test_flatten_data_sem_preserva_nulos_mantem_o_comportamento_atual() -> None:
    achatado = ClientPostgresDB("dsn")._flatten_data([{"a": None}, {"a": "x"}])

    assert achatado[1] == {"a": "x"}
    assert achatado[0]["a"] != achatado[0]["a"]  # NaN, como antes


def test_flatten_data_preserva_nulos_troca_nan_por_none() -> None:
    achatado = ClientPostgresDB("dsn")._flatten_data(
        [{"a": None, "b": {"c": 1}}, {"a": "x", "b": {"c": None}}], preserva_nulos=True
    )

    assert achatado == [{"a": None, "b__c": 1.0}, {"a": "x", "b__c": None}]


# ---------------------------------------------------------------------------
# replace_rows
# ---------------------------------------------------------------------------
def test_replace_rows_sem_dados_nao_toca_no_banco() -> None:
    with patch("cliente_postgres.psycopg2.connect") as mock_connect:
        apagadas = ClientPostgresDB("dsn").replace_rows(
            [], "termos_execucao", delete_where="true", delete_params=()
        )

    assert apagadas == 0
    mock_connect.assert_not_called()


@pytest.mark.parametrize(
    "schema, tabela", [("transfere_gov; drop", "t"), ("transfere_gov", "t-1")]
)
def test_replace_rows_rejeita_identificador_invalido(schema: str, tabela: str) -> None:
    with patch("cliente_postgres.psycopg2.connect") as mock_connect:
        with pytest.raises(ValueError):
            ClientPostgresDB("dsn").replace_rows(
                [{"a": "1"}], tabela, delete_where="true", delete_params=(), schema=schema
            )

    mock_connect.assert_not_called()


def test_replace_rows_apaga_e_insere_na_mesma_transacao() -> None:
    conn = _mock_connect([], [])
    conn.cursor.return_value.rowcount = 5
    db = ClientPostgresDB("dsn")
    linhas = [{"id_termo": "1", "dt": None}]

    with (
        patch("cliente_postgres.psycopg2.connect", return_value=conn),
        patch.object(db, "create_table_if_not_exists"),
        patch.object(db, "alter_table"),
        patch.object(db, "insert_data") as mock_insert,
    ):
        apagadas = db.replace_rows(
            linhas,
            "termos_execucao",
            delete_where="true",
            delete_params=(),
            schema="transfere_gov",
            preserva_nulos=True,
        )

    assert apagadas == 5
    conn.cursor.return_value.execute.assert_called_once_with(
        "DELETE FROM transfere_gov.termos_execucao WHERE true", ()
    )
    mock_insert.assert_called_once_with(
        linhas,
        "termos_execucao",
        schema="transfere_gov",
        conn=conn,
        preserva_nulos=True,
    )
    conn.commit.assert_called_once()
