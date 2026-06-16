import pytest
from unittest.mock import patch, MagicMock
from cliente_ibge import ClienteIBGE

DB = "teste"
FTP_HOST = "ftp.ibge.gov.br"
BASE_DIR = "/Censos/Censo_Demografico_2022/"


@pytest.fixture
def cliente_ibge() -> ClienteIBGE:
    return ClienteIBGE(database=DB)


@pytest.fixture
def mock_ftp():
    with patch("cliente_ibge.FTP") as MockFTP:
        mock_ftp_instance = MagicMock()
        MockFTP.return_value = mock_ftp_instance
        yield mock_ftp_instance


def test_init_cliente_ibge(cliente_ibge):
    assert cliente_ibge.host == FTP_HOST
    assert cliente_ibge.database == DB


# ---------------------------------------------------------------------------
# _conectar
# ---------------------------------------------------------------------------


def test_conectar_estabelece_conexao(cliente_ibge: ClienteIBGE, mock_ftp) -> None:

    with cliente_ibge._conectar() as ftp:
        assert ftp == mock_ftp

    mock_ftp.connect.assert_called_once_with(FTP_HOST)
    mock_ftp.login.assert_called_once_with(user="anonymous", passwd="anonymous@")
    mock_ftp.set_pasv.assert_called_once_with(True)
    mock_ftp.cwd.assert_called_once_with(BASE_DIR + DB)

    mock_ftp.quit.assert_called_once()
    mock_ftp.close.assert_not_called()


def test_conectar_trata_excecao_conexao(cliente_ibge: ClienteIBGE, mock_ftp) -> None:
    mock_ftp.connect.side_effect = Exception("Erro de conexão")

    with pytest.raises(Exception) as exc_info:
        with cliente_ibge._conectar():
            pass

    assert str(exc_info.value) == "Erro de conexão"
    mock_ftp.connect.assert_called_once_with(FTP_HOST)
    mock_ftp.login.assert_not_called()
    mock_ftp.set_pasv.assert_not_called()
    mock_ftp.cwd.assert_not_called()
    mock_ftp.quit.assert_called_once()
    mock_ftp.close.assert_not_called()


def test_conectar_trata_excecao_quit(cliente_ibge: ClienteIBGE, mock_ftp) -> None:
    mock_ftp.quit.side_effect = Exception("Erro ao fechar conexão")

    with cliente_ibge._conectar():
        pass

    mock_ftp.quit.assert_called_once()
    mock_ftp.close.assert_called_once()


# ---------------------------------------------------------------------------
# listar_arquivos_alvo
# ---------------------------------------------------------------------------


def test_listar_arquivos_alvo_filtra_arquivos(
    cliente_ibge: ClienteIBGE, mock_ftp
) -> None:
    mock_ftp.nlst.return_value = [
        "dados.xlsx",
        "dados.csv",
        "dados.txt",
        "imagem.png",
        "relatorio.xls",
    ]

    arquivos = cliente_ibge.listar_arquivos_alvo()
    assert arquivos == ["dados.xlsx", "dados.csv", "relatorio.xls"]
    mock_ftp.nlst.assert_called_once()


def test_listar_arquivos_alvo_sem_arquivos(cliente_ibge: ClienteIBGE, mock_ftp) -> None:
    mock_ftp.nlst.return_value = ["dados.txt", "imagem.png"]

    arquivos = cliente_ibge.listar_arquivos_alvo()
    assert arquivos == []
    mock_ftp.nlst.assert_called_once()


def test_listar_arquivos_alvo_trata_excecao(cliente_ibge: ClienteIBGE, mock_ftp) -> None:
    mock_ftp.nlst.side_effect = Exception("Erro ao listar arquivos")

    arquivos = cliente_ibge.listar_arquivos_alvo()
    assert arquivos == []
    mock_ftp.nlst.assert_called_once()


# ---------------------------------------------------------------------------
# listar_arquivos_em_subpastas
# ---------------------------------------------------------------------------


def test_listar_arquivos_em_subpastas_com_sucesso(
    cliente_ibge: ClienteIBGE, mock_ftp
) -> None:

    mock_ftp.nlst.side_effect = [
        [".", "..", "dado_A.xlsx", "leia_me.txt"],
        ["dado_B1.csv", "dado_B2.xls"],
        ["dado_C.pdf"],
    ]

    resultado = cliente_ibge.listar_arquivos_em_subpastas(
        subpastas=["pastaA", "pastaB", "pastaC"],
        extensoes=(".xlsx", ".xls", ".csv"),
        formato_preferido="xlsx",
    )

    expect_resultado = [
        {"subcaminho": "pastaA/xlsx", "arquivo": "dado_A.xlsx"},
        {"subcaminho": "pastaB/xlsx", "arquivo": "dado_B1.csv"},
        {"subcaminho": "pastaB/xlsx", "arquivo": "dado_B2.xls"},
    ]

    assert resultado == expect_resultado

    # 1 chamada para cada subpasta + 1 chamada para o diretório base
    assert mock_ftp.cwd.call_count == 4

    mock_ftp.cwd.assert_any_call(BASE_DIR + DB + "/pastaA/xlsx")
    mock_ftp.cwd.assert_any_call(BASE_DIR + DB + "/pastaB/xlsx")
    mock_ftp.cwd.assert_any_call(BASE_DIR + DB + "/pastaC/xlsx")

    assert mock_ftp.nlst.call_count == 3


def test_listar_arquivos_em_subpastas_sem_formato_preferido(
    cliente_ibge: ClienteIBGE, mock_ftp
) -> None:
    mock_ftp.nlst.side_effect = [
        ["dado_A.xlsx", "dado_B.csv", "dado_C.txt"],
        ["leia_me.txt"],
    ]

    resultado = cliente_ibge.listar_arquivos_em_subpastas(
        subpastas=["pastaA", "pastaB"],
        extensoes=(".xlsx", ".csv"),
        formato_preferido=None,
    )

    expect_resultado = [
        {"subcaminho": "pastaA", "arquivo": "dado_A.xlsx"},
        {"subcaminho": "pastaA", "arquivo": "dado_B.csv"},
    ]

    assert resultado == expect_resultado

    # 1 chamada para cada subpasta + 1 chamada para o diretório base
    assert mock_ftp.cwd.call_count == 3
    mock_ftp.cwd.assert_any_call(BASE_DIR + DB + "/pastaA")
    mock_ftp.cwd.assert_any_call(BASE_DIR + DB + "/pastaB")
    assert mock_ftp.nlst.call_count == 2
