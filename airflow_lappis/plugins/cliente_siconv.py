import zipfile
import csv
import io
import logging
import requests


class ClienteSiconv:
    URL_ZIP = "https://repositorio.dados.gov.br/seges/detru/siconv.zip"
    ZIP_PATH = "/tmp/siconv.zip"

    def baixar_zip(self) -> None:
        logging.info("[cliente_siconv.py] Baixando arquivo SICONV...")
        response = requests.get(self.URL_ZIP, stream=True)
        response.raise_for_status()
        with open(self.ZIP_PATH, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logging.info("[cliente_siconv.py] Download concluído")

    def ler_csv(self, nome_csv: str, skip_rows: int = 0, colunas_esperadas: list = None):
        logging.info(f"[cliente_siconv.py] Lendo {nome_csv} em modo streaming...")
        with zipfile.ZipFile(self.ZIP_PATH, "r") as z:
            with z.open(nome_csv) as f:
                conteudo = io.TextIOWrapper(f, encoding="utf-8-sig")
                reader = csv.DictReader(conteudo, delimiter=";")

                if colunas_esperadas:
                    colunas_csv = reader.fieldnames or []
                    faltando = [c for c in colunas_esperadas if c not in colunas_csv]
                    if faltando:
                        raise ValueError(
                            f"[cliente_siconv.py] Colunas faltando em {nome_csv}: "
                            f"{faltando}"
                        )

                for i, row in enumerate(reader):
                    if i < skip_rows:
                        continue

                    if colunas_esperadas:
                        yield {k.lower(): row[k] for k in colunas_esperadas}
                    else:
                        yield {k.lower(): v for k, v in row.items() if k is not None}
