import logging
import io
import os
import zipfile
from typing import Optional, cast, List, Dict

import chardet
import pandas as pd
import pytz
from datetime import datetime
from pandas.errors import EmptyDataError
from imap_tools import MailBox, AND

# Configuração do log
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def format_csv(
    csv_data: str, column_mapping: Optional[Dict[int, str]], skiprows: int
) -> pd.DataFrame:
    """Formata um arquivo CSV conforme mapeamento de colunas."""
    if column_mapping:
        df = pd.read_csv(io.StringIO(csv_data), skiprows=skiprows, header=None)
        column_names: List[str] = [
            column_mapping.get(i, f"col_{i}") for i in range(len(df.columns))
        ]
        df.columns = pd.Index(column_names)
    else:
        df = pd.read_csv(io.StringIO(csv_data), skiprows=skiprows, header=0)
    return df


def extract_csv_from_zip(
    zip_payload: bytes, column_mapping: dict, skiprows: int = 0
) -> Optional[pd.DataFrame]:
    """Extrai e formata o primeiro arquivo CSV encontrado em um ZIP."""
    with zipfile.ZipFile(io.BytesIO(zip_payload)) as zip_file:
        for file_name in zip_file.namelist():
            if file_name.lower().endswith(".csv"):
                raw_data = zip_file.read(file_name)
                encoding = chardet.detect(raw_data)["encoding"]

                if not raw_data.strip():
                    logging.warning("CSV vazio no anexo ZIP: %s", file_name)
                    continue

                try:
                    decoded_data = raw_data.decode(encoding or "utf-8", errors="replace")
                    if not decoded_data.strip():
                        logging.warning("CSV vazio no anexo ZIP: %s", file_name)
                        continue
                    return format_csv(decoded_data, column_mapping, skiprows)
                except EmptyDataError:
                    logging.warning(
                        "CSV sem colunas apos skiprows=%s no arquivo: %s",
                        skiprows,
                        file_name,
                    )
                    continue
    return None


def fetch_email_with_zip(
    imap_server: str, email: str, password: str, sender_email: str, subject: str
) -> List[bytes]:
    """Busca todos os e-mails do dia atual e retorna todos os anexos ZIP."""
    today = datetime.now(pytz.timezone("America/Sao_Paulo")).date()
    zip_payloads: List[bytes] = []
    with MailBox(imap_server).login(email, password) as mailbox:
        for msg in mailbox.fetch(AND(date=today, from_=sender_email, subject=subject)):
            for attachment in msg.attachments:
                if attachment.filename.lower().endswith(".zip"):
                    zip_payloads.append(cast(bytes, attachment.payload))
    return zip_payloads


def fetch_and_process_email(
    imap_server: str,
    email: str,
    password: str,
    sender_email: str,
    subject: str,
    column_mapping: dict,
    skiprows: int = 0,
    output_path: Optional[str] = None,
) -> Optional[str]:
    """Busca e processa e-mails do dia, extraindo CSVs de todos os ZIPs anexados."""
    try:
        zip_payloads = fetch_email_with_zip(
            imap_server, email, password, sender_email, subject
        )
        if not zip_payloads:
            logging.warning("Nenhum anexo ZIP encontrado.")
            return None

        if not output_path:
            raise ValueError(
                "output_path é obrigatório em fetch_and_process_email; "
            )

        logging.info("Total de anexos ZIP encontrados: %s", len(zip_payloads))
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        first_chunk = True
        total_rows = 0

        for idx, zip_payload in enumerate(zip_payloads, start=1):
            csv_df = extract_csv_from_zip(zip_payload, column_mapping, skiprows)
            if csv_df is not None and not csv_df.empty:
                rows = len(csv_df)
                total_rows += rows
                csv_df.to_csv(
                    output_path,
                    mode="w" if first_chunk else "a",
                    header=first_chunk,
                    index=False,
                )
                first_chunk = False
                logging.info(
                    "CSV do ZIP %s processado e gravado em disco (%s linhas).",
                    idx,
                    rows,
                )
            else:
                logging.warning(
                    "ZIP %s ignorado por nao conter CSV valido.",
                    idx,
                )

        if total_rows == 0:
            logging.warning("Nenhum CSV processado.")
            return None

        logging.info(
            "Processamento concluido. Total de linhas gravadas em %s: %s",
            output_path,
            total_rows,
        )
        return output_path
    except Exception as e:
        logging.error(f"Erro ao processar e-mails: {e}")
        raise
