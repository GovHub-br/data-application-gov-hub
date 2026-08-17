from ftplib import error_perm
import io

import pytest
from unittest.mock import patch, MagicMock
from cliente_ibge import ClienteIBGE

DB = "test"
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


def test_conectar_establishes_connection(cliente_ibge: ClienteIBGE, mock_ftp) -> None:

    with cliente_ibge._conectar() as ftp:
        assert ftp == mock_ftp

    mock_ftp.connect.assert_called_once_with(FTP_HOST)
    mock_ftp.login.assert_called_once_with(
        user=ClienteIBGE.FTP_USER, passwd=ClienteIBGE.FTP_PASS
    )
    mock_ftp.set_pasv.assert_called_once_with(True)
    mock_ftp.cwd.assert_called_once_with(BASE_DIR + DB)

    mock_ftp.quit.assert_called_once()
    mock_ftp.close.assert_not_called()


def test_conectar_establishes_connection_with_subpath(
    cliente_ibge: ClienteIBGE, mock_ftp
) -> None:

    with cliente_ibge._conectar(subcaminho="subfolder") as ftp:
        assert ftp == mock_ftp

    mock_ftp.cwd.assert_called_once_with(BASE_DIR + DB + "/subfolder")

    mock_ftp.quit.assert_called_once()
    mock_ftp.close.assert_not_called()


def test_conectar_handles_connection_exception(
    cliente_ibge: ClienteIBGE, mock_ftp
) -> None:

    error_msg = "Connection error"
    mock_ftp.connect.side_effect = Exception(error_msg)

    with pytest.raises(Exception) as exc_info:
        with cliente_ibge._conectar():
            assert str(exc_info.value) == error_msg

    mock_ftp.connect.assert_called_once_with(FTP_HOST)
    mock_ftp.login.assert_not_called()
    mock_ftp.set_pasv.assert_not_called()
    mock_ftp.cwd.assert_not_called()
    mock_ftp.quit.assert_called_once()
    mock_ftp.close.assert_not_called()


def test_conectar_handles_quit_exception(cliente_ibge: ClienteIBGE, mock_ftp) -> None:
    mock_ftp.quit.side_effect = Exception("Error closing connection")

    with cliente_ibge._conectar() as ftp:
        assert ftp == mock_ftp

    mock_ftp.quit.assert_called_once()
    mock_ftp.close.assert_called_once()


# ---------------------------------------------------------------------------
# listar_arquivos_alvo
# ---------------------------------------------------------------------------


def test_listar_arquivos_alvo_filters_files(cliente_ibge: ClienteIBGE, mock_ftp) -> None:
    """
    -> Normal execution with a mix of target and non-target files in the listing.
    """
    mock_ftp.nlst.return_value = [
        "data.xlsx",
        "data.csv",
        "data.txt",
        "image.png",
        "report.xls",
    ]

    files = cliente_ibge.listar_arquivos_alvo()
    assert files == ["data.xlsx", "data.csv", "report.xls"]
    mock_ftp.nlst.assert_called_once()


def test_listar_arquivos_alvo_without_files(cliente_ibge: ClienteIBGE, mock_ftp) -> None:
    mock_ftp.nlst.return_value = ["data.txt", "image.png"]

    files = cliente_ibge.listar_arquivos_alvo()
    assert files == []
    mock_ftp.nlst.assert_called_once()


def test_listar_arquivos_alvo_handles_exception(
    cliente_ibge: ClienteIBGE, mock_ftp, caplog
) -> None:

    error_msg = "Error listing files"
    mock_ftp.nlst.side_effect = Exception(error_msg)

    with caplog.at_level("ERROR"):
        files = cliente_ibge.listar_arquivos_alvo()

    assert files == []
    mock_ftp.nlst.assert_called_once()

    assert error_msg in caplog.text


# ---------------------------------------------------------------------------
# listar_arquivos_em_subpastas
# ---------------------------------------------------------------------------


def test_listar_arquivos_em_subpastas_successfully(
    cliente_ibge: ClienteIBGE, mock_ftp
) -> None:
    """
    -> Normal execution with valid files in subfolders.
        --> validates subfolder slash trimming with strip()
        --> ignores non-target files and directories in the listing
    """

    mock_ftp.nlst.side_effect = [
        [".", "..", "data_A.xlsx", "leia_me.txt"],
        ["data_B1.csv", "data_B2.xls"],
        ["data_C.pdf"],
    ]

    result = cliente_ibge.listar_arquivos_em_subpastas(
        subpastas=["folderA", "folderB", "folderC"],
        extensoes=(".xlsx", ".xls", ".csv"),
        formato_preferido="xlsx",
    )

    expected_result = [
        {"subcaminho": "folderA/xlsx", "arquivo": "data_A.xlsx"},
        {"subcaminho": "folderB/xlsx", "arquivo": "data_B1.csv"},
        {"subcaminho": "folderB/xlsx", "arquivo": "data_B2.xls"},
    ]

    assert result == expected_result

    # 1 call for each subfolder + 1 call for the base directory
    assert mock_ftp.cwd.call_count == 4

    mock_ftp.cwd.assert_any_call(BASE_DIR + DB + "/folderA/xlsx")
    mock_ftp.cwd.assert_any_call(BASE_DIR + DB + "/folderB/xlsx")
    mock_ftp.cwd.assert_any_call(BASE_DIR + DB + "/folderC/xlsx")

    assert mock_ftp.nlst.call_count == 3


def test_listar_arquivos_em_subpastas_without_preferred_format(
    cliente_ibge: ClienteIBGE, mock_ftp
) -> None:
    mock_ftp.nlst.side_effect = [
        ["data_A1.xlsx", "data_A2.csv", "data_A3.txt"],
        ["readme.txt"],
    ]

    result = cliente_ibge.listar_arquivos_em_subpastas(
        subpastas=["folderA", "folderB"],
        extensoes=(".xlsx", ".csv"),
        formato_preferido=None,
    )

    expected_result = [
        {"subcaminho": "folderA", "arquivo": "data_A1.xlsx"},
        {"subcaminho": "folderA", "arquivo": "data_A2.csv"},
    ]

    assert result == expected_result

    # 1 call for each subfolder + 1 call for the base directory
    assert mock_ftp.cwd.call_count == 3
    mock_ftp.cwd.assert_any_call(BASE_DIR + DB + "/folderA")
    mock_ftp.cwd.assert_any_call(BASE_DIR + DB + "/folderB")
    assert mock_ftp.nlst.call_count == 2


def test_listar_arquivos_em_subpastas_without_valid_files(
    cliente_ibge: ClienteIBGE, mock_ftp
) -> None:
    mock_ftp.nlst.side_effect = [
        ["data_A.txt", "readme.txt"],
        [".", "data_B.pdf", "subfolder"],
    ]

    result = cliente_ibge.listar_arquivos_em_subpastas(
        subpastas=["folderA", "folderB"],
        extensoes=(".xlsx", ".csv"),
        formato_preferido=None,
    )

    assert result == []
    # 1 call for each subfolder + 1 call for the base directory
    assert mock_ftp.cwd.call_count == 3
    mock_ftp.cwd.assert_any_call(BASE_DIR + DB + "/folderA")
    mock_ftp.cwd.assert_any_call(BASE_DIR + DB + "/folderB")
    assert mock_ftp.nlst.call_count == 2


def test_listar_arquivos_em_subpastas_with_inaccessible_subfolder(
    cliente_ibge: ClienteIBGE, mock_ftp, caplog
) -> None:

    error_msg = "550 Failed to change directory."

    mock_ftp.cwd.side_effect = [
        None,  # _conectar() to base directory
        None,
        error_perm(error_msg),
    ]

    mock_ftp.nlst.return_value = ["data_A.xlsx", "readme.txt"]

    with caplog.at_level("WARNING"):
        resultado = cliente_ibge.listar_arquivos_em_subpastas(
            subpastas=["exists", "not-exists"],
            extensoes=(".xlsx",),
            formato_preferido="xlsx",
        )

    expected_result = [
        {"subcaminho": "exists/xlsx", "arquivo": "data_A.xlsx"},
    ]

    assert resultado == expected_result

    # 1 call for each subfolder + 1 call for the base directory
    assert mock_ftp.cwd.call_count == 3
    mock_ftp.cwd.assert_any_call(BASE_DIR + DB + "/not-exists/xlsx")
    mock_ftp.cwd.assert_any_call(BASE_DIR + DB + "/exists/xlsx")
    assert mock_ftp.nlst.call_count == 1

    assert BASE_DIR + DB + "/not-exists/xlsx" in caplog.text
    assert error_msg in caplog.text


def test_listar_arquivos_em_subpastas_handles_exception(
    cliente_ibge: ClienteIBGE, mock_ftp, caplog
) -> None:

    error_msg = "Error listing files"

    mock_ftp.nlst.side_effect = Exception(error_msg)

    with caplog.at_level("ERROR"):
        resultado = cliente_ibge.listar_arquivos_em_subpastas(
            subpastas=["folderA"],
            extensoes=(".xlsx",),
            formato_preferido="xlsx",
        )

    assert resultado == []
    mock_ftp.cwd.assert_any_call(BASE_DIR + DB + "/folderA/xlsx")
    mock_ftp.nlst.assert_called_once()

    assert error_msg in caplog.text


# ---------------------------------------------------------------------------
# listar_arquivos_texto
# ---------------------------------------------------------------------------


def test_listar_arquivos_texto_successfully(cliente_ibge: ClienteIBGE, mock_ftp) -> None:
    """
    -> Normal execution with text files present in the listing.
        --> ignores missing input entries
        --> validates subfolder slash trimming with strip()
    """

    entries = [
        ("folderA", "index_a1.txt"),
        ("folderA", "index_a2.txt"),
        ("folderB", "dont_exist.txt"),
        ("folderC/", "index_c.txt"),
    ]

    mock_ftp.nlst.side_effect = [
        ["index_a1.txt"],
        ["index_a2.txt"],
        ["other.txt"],
        ["index_c.txt"],
    ]

    result = cliente_ibge.listar_arquivos_texto(entries)

    expected_result = [
        {"subcaminho": "folderA", "arquivo": "index_a1.txt"},
        {"subcaminho": "folderA", "arquivo": "index_a2.txt"},
        {"subcaminho": "folderC", "arquivo": "index_c.txt"},
    ]

    assert result == expected_result
    # 1 call for each entry + 1 call for the base directory
    assert mock_ftp.cwd.call_count == 5
    assert mock_ftp.nlst.call_count == 4


def test_listar_arquivos_texto_without_existing_files(
    cliente_ibge: ClienteIBGE, mock_ftp
) -> None:
    entries = [
        ("folderA", "index_a.txt"),
        ("folderB", "index_b.txt"),
    ]

    mock_ftp.nlst.side_effect = [
        ["other.txt"],
        ["another.txt"],
    ]

    result = cliente_ibge.listar_arquivos_texto(entries)

    assert result == []
    # 1 call for each entry + 1 call for the base directory
    assert mock_ftp.cwd.call_count == 3
    assert mock_ftp.nlst.call_count == 2


def test_listar_arquivos_texto_invalid_folder(
    cliente_ibge: ClienteIBGE, mock_ftp, caplog
) -> None:

    error_msg = "550 Failed to change directory."

    entries = [
        ("folderA", "index_a.pdf"),
        ("invalid_folder", "index_b.pdf"),
    ]

    mock_ftp.cwd.side_effect = [
        None,  # _conectar() to base directory
        None,
        error_perm(error_msg),
    ]

    mock_ftp.nlst.side_effect = [["index_a.pdf"]]

    with caplog.at_level("WARNING"):
        result = cliente_ibge.listar_arquivos_texto(entries)

    expected_result = [
        {"subcaminho": "folderA", "arquivo": "index_a.pdf"},
    ]

    assert result == expected_result
    # 1 call for each entry + 1 call for the base directory
    assert mock_ftp.cwd.call_count == 3
    assert mock_ftp.nlst.call_count == 1

    assert BASE_DIR + DB + "/invalid_folder" in caplog.text
    assert error_msg in caplog.text


def test_listar_arquivos_texto_handles_exception(
    cliente_ibge: ClienteIBGE, mock_ftp, caplog
) -> None:

    entries = [
        ("folderA", "index_a.pdf"),
        ("folderB", "index_b.pdf"),
    ]

    error_msg = "Connection error"
    mock_ftp.connect.side_effect = Exception(error_msg)

    with caplog.at_level("ERROR"):
        result = cliente_ibge.listar_arquivos_texto(entries)

    assert result == []

    mock_ftp.connect.assert_called_once()
    mock_ftp.cwd.assert_not_called()
    mock_ftp.nlst.assert_not_called()

    assert error_msg in caplog.text


# ---------------------------------------------------------------------------
# obter_conteudo_arquivo
# ---------------------------------------------------------------------------


def test_obter_conteudo_arquivo_successfully(
    cliente_ibge: ClienteIBGE, mock_ftp, caplog
) -> None:

    file_content = b"Test file content"

    file = "data.csv"

    def mock_retrbinary(_, callback):
        callback(file_content)

    mock_ftp.retrbinary.side_effect = mock_retrbinary

    with caplog.at_level("INFO"):
        buffer = cliente_ibge.obter_conteudo_arquivo(file)

    assert isinstance(buffer, io.BytesIO)
    assert buffer.tell() == 0  # moves buffer to the beginning after writing
    assert buffer.read() == file_content
    mock_ftp.retrbinary.assert_called_once_with("RETR " + file, buffer.write)

    mock_ftp.cwd.assert_called_once_with(BASE_DIR + DB)

    assert f"./{file}" in caplog.text


def test_obter_conteudo_arquivo_with_subcaminho(
    cliente_ibge: ClienteIBGE, mock_ftp, caplog
) -> None:

    file_content = b""

    file = "data.xlsx"
    subcaminho = "folderA"

    def mock_retrbinary(_, callback):
        callback(file_content)

    mock_ftp.retrbinary.side_effect = mock_retrbinary

    with caplog.at_level("INFO"):
        buffer = cliente_ibge.obter_conteudo_arquivo(file, subcaminho=subcaminho)

    assert isinstance(buffer, io.BytesIO)
    assert buffer.tell() == 0  # moves buffer to the beginning after writing
    assert buffer.read() == file_content
    mock_ftp.retrbinary.assert_called_once_with("RETR " + file, buffer.write)

    mock_ftp.cwd.assert_called_once_with(BASE_DIR + DB + "/" + subcaminho)

    assert f"{subcaminho}/{file}" in caplog.text


def test_obter_conteudo_arquivo_handles_exception(
    cliente_ibge: ClienteIBGE, mock_ftp, caplog
) -> None:

    file = "data.txt"

    error_msg = "Error downloading file"
    mock_ftp.retrbinary.side_effect = Exception(error_msg)

    with caplog.at_level("ERROR"):
        buffer = cliente_ibge.obter_conteudo_arquivo(file)

    assert buffer is None
    mock_ftp.retrbinary.assert_called_once()
    assert error_msg in caplog.text


# ---------------------------------------------------------------------------
# obter_conteudo_texto
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_obter_conteudo(cliente_ibge: ClienteIBGE):
    with patch.object(cliente_ibge, "obter_conteudo_arquivo") as mock_metodo:
        yield mock_metodo


def test_obter_conteudo_texto_with_nonexistent_file(
    cliente_ibge: ClienteIBGE, mock_obter_conteudo
) -> None:

    file = "nonexistent.txt"
    subpath = "folder/subfolder"

    mock_obter_conteudo.return_value = None

    result = cliente_ibge.obter_conteudo_texto(file, subcaminho=subpath)

    assert result is None
    mock_obter_conteudo.assert_called_once_with(file, subcaminho=subpath)


def test_obter_conteudo_texto_decodes_utf8(
    cliente_ibge: ClienteIBGE, mock_obter_conteudo
) -> None:

    file = "data.txt"

    mock_obter_conteudo.return_value = io.BytesIO(
        b"Dados atuais com a\xc3\xa7\xc3\xa3o"  # "ação" in utf-8
    )

    result = cliente_ibge.obter_conteudo_texto(file)

    assert result == "Dados atuais com ação"
    mock_obter_conteudo.assert_called_once_with(file, subcaminho="")


def test_obter_conteudo_texto_decodes_cp1252_fallback(
    cliente_ibge: ClienteIBGE, mock_obter_conteudo
) -> None:

    mock_obter_conteudo.return_value = io.BytesIO(
        b"Dados Windows com a\xe7\xe3o"  # bytes (\xe7\xe3 == çã) doesnt exist in uft-8
    )

    result = cliente_ibge.obter_conteudo_texto("Windows.txt")

    assert result == "Dados Windows com ação"


def test_obter_conteudo_texto_decodes_latin1(
    cliente_ibge: ClienteIBGE, mock_obter_conteudo
) -> None:

    mock_obter_conteudo.return_value = io.BytesIO(
        b"Dado \x81 legado"  # \x81 causes error in uft-8 AND cp1252
    )

    result = cliente_ibge.obter_conteudo_texto("unsualfile.txt")

    assert result == "Dado \x81 legado"
