import io
import logging
from contextlib import contextmanager

# ftp.ibge.gov.br é um servidor público do governo
# brasileiro que não oferece suporte a FTPS/SFTP. Apenas dados
# públicos e anônimos são trafegados nessa conexão.
from ftplib import FTP  # NOSONAR

from cliente_base import ClienteBase


class ClienteIBGE(ClienteBase):
    FTP_HOST = "ftp.ibge.gov.br"
    BASE_DIR = "/Censos/Censo_Demografico_2022/"

    def __init__(self, database: str) -> None:
        self.host = ClienteIBGE.FTP_HOST
        self.database = database
        logging.info("[cliente_ibge] Inicializando conexão FTP com: %s", self.host)

    @contextmanager
    def _conectar(self, subcaminho: str = ""):
        """
        Abre uma conexão FTP com o servidor público do IBGE.

        Uso:
            with self._conectar() as ftp:
                ftp.nlst()
        """
        full_path = self._caminho_remoto(subcaminho)
        ftp = FTP(timeout=30)  # NOSONAR
        try:
            ftp.connect(self.host)
            resp = ftp.login(user="anonymous", passwd="anonymous@")
            logging.info("[cliente_ibge] FTP login: %s", resp)
            ftp.set_pasv(True)
            ftp.cwd(full_path)
            yield ftp
        finally:
            try:
                ftp.quit()
            except Exception:
                ftp.close()

    # Interface pública
    def listar_arquivos_alvo(self) -> list[str]:
        """Lista arquivos Excel/CSV do diretório do Censo 2022."""
        try:
            with self._conectar() as ftp:
                arquivos = ftp.nlst()

            filtrados = [f for f in arquivos if f.endswith((".xlsx", ".xls", ".csv"))]
            logging.info("[cliente_ibge] %d arquivo(s) encontrado(s).", len(filtrados))
            return filtrados

        except Exception as exc:
            logging.error("[cliente_ibge] Erro ao listar arquivos: %s", exc)
            return []

    def _caminho_remoto(self, subcaminho: str = "") -> str:
        base = f"{self.BASE_DIR.rstrip('/')}/{self.database.lstrip('/')}"
        if not subcaminho:
            return base
        return f"{base}/{subcaminho.strip('/')}"

    def listar_arquivos_em_subpastas(
        self,
        subpastas: list[str],
        extensoes: tuple[str, ...] = (".xlsx", ".xls", ".csv"),
        formato_preferido: str | None = "xlsx",
    ) -> list[dict[str, str]]:
        """
        Lista arquivos alvo em subpastas relativas ao diretório do tema.

        Retorna lista de dicts com chaves ``subcaminho`` e ``arquivo``.
        Quando ``formato_preferido`` é informado, prioriza arquivos dessa
        subpasta (ex.: ``xlsx`` em vez de ``ods``).
        """
        resultado: list[dict[str, str]] = []
        try:
            with self._conectar() as ftp:
                base = self._caminho_remoto()
                for subpasta in subpastas:
                    caminho = f"{base}/{subpasta.strip('/')}"
                    if formato_preferido:
                        caminho = f"{caminho}/{formato_preferido}"
                    try:
                        ftp.cwd(caminho)
                    except Exception as exc:
                        logging.warning(
                            "[cliente_ibge] Subpasta inacessível '%s': %s",
                            caminho,
                            exc,
                        )
                        continue

                    for nome in ftp.nlst():
                        if nome in (".", ".."):
                            continue
                        if nome.endswith(extensoes):
                            resultado.append(
                                {
                                    "subcaminho": (
                                        f"{subpasta.strip('/')}/{formato_preferido}"
                                        if formato_preferido
                                        else subpasta.strip("/")
                                    ),
                                    "arquivo": nome,
                                }
                            )

            logging.info(
                "[cliente_ibge] %d arquivo(s) em subpastas: %s",
                len(resultado),
                subpastas,
            )
            return resultado

        except Exception as exc:
            logging.error("[cliente_ibge] Erro ao listar subpastas: %s", exc)
            return []

    def listar_arquivos_texto(
        self, entradas: list[tuple[str, str]]
    ) -> list[dict[str, str]]:
        """
        Lista arquivos de texto (índices) em subpastas.

        ``entradas`` é uma lista de tuplas ``(subpasta, nome_arquivo)``.
        """
        encontrados: list[dict[str, str]] = []
        try:
            with self._conectar() as ftp:
                base = self._caminho_remoto()
                for subpasta, nome_arquivo in entradas:
                    caminho = f"{base}/{subpasta.strip('/')}"
                    try:
                        ftp.cwd(caminho)
                        if nome_arquivo in ftp.nlst():
                            encontrados.append(
                                {
                                    "subcaminho": subpasta.strip("/"),
                                    "arquivo": nome_arquivo,
                                }
                            )
                    except Exception as exc:
                        logging.warning(
                            "[cliente_ibge] Índice não encontrado em '%s': %s",
                            caminho,
                            exc,
                        )
            return encontrados
        except Exception as exc:
            logging.error("[cliente_ibge] Erro ao listar índices: %s", exc)
            return []

    def obter_conteudo_arquivo(
        self, nome_arquivo: str, subcaminho: str = ""
    ) -> io.BytesIO | None:
        """Baixa um arquivo do FTP diretamente para memória."""
        buffer = io.BytesIO()
        try:
            with self._conectar(subcaminho=subcaminho) as ftp:
                logging.info(
                    "[cliente_ibge] Baixando: %s/%s", subcaminho or ".", nome_arquivo
                )
                ftp.retrbinary(f"RETR {nome_arquivo}", buffer.write)

            buffer.seek(0)
            return buffer

        except Exception as exc:
            logging.error("[cliente_ibge] Erro ao baixar '%s': %s", nome_arquivo, exc)
            return None

    def obter_conteudo_texto(
        self, nome_arquivo: str, subcaminho: str = ""
    ) -> str | None:
        """Baixa um arquivo de texto do FTP e retorna o conteúdo decodificado."""
        buffer = self.obter_conteudo_arquivo(nome_arquivo, subcaminho=subcaminho)
        if not buffer:
            return None
        raw = buffer.read()
        for encoding in ("utf-8", "latin-1", "cp1252"):
            try:
                return raw.decode(encoding)
            except UnicodeDecodeError:
                continue
        return raw.decode("latin-1", errors="replace")
