import logging
import io
import zipfile
from typing import Optional, cast, List, Dict
import pandas as pd
from pandas.errors import EmptyDataError
from imap_tools import MailBox, AND
import chardet
from datetime import datetime, date
import pytz

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
    imap_server: str,
    email: str,
    password: str,
    sender_email: str,
    subject: str,
    target_date: Optional[date] = None,
) -> List[bytes]:
    """Busca e-mails da data alvo (ou dia atual) e retorna os anexos ZIP."""
    query_date = target_date or datetime.now(pytz.timezone("America/Sao_Paulo")).date()
    zip_payloads: List[bytes] = []
    with MailBox(imap_server).login(email, password) as mailbox:
        # bulk=True: single IMAP FETCH command for all messages (avoids overquota)
        for msg in mailbox.fetch(
            AND(date=query_date, from_=sender_email, subject=subject),
            bulk=True,
        ):
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
    target_date: Optional[date] = None,
) -> Optional[str]:
    """Busca e processa e-mails da data alvo (ou dia atual), extraindo CSVs."""
    try:
        zip_payloads = fetch_email_with_zip(
            imap_server,
            email,
            password,
            sender_email,
            subject,
            target_date=target_date,
        )
        if not zip_payloads:
            logging.warning("Nenhum anexo ZIP encontrado.")
            return None

        logging.info("Total de anexos ZIP encontrados: %s", len(zip_payloads))

        dataframes: List[pd.DataFrame] = []
        for idx, zip_payload in enumerate(zip_payloads, start=1):
            csv_data = extract_csv_from_zip(zip_payload, column_mapping, skiprows)
            if csv_data is not None:
                dataframes.append(csv_data)
            else:
                logging.warning(
                    "ZIP %s ignorado por nao conter CSV valido.",
                    idx,
                )

        if dataframes:
            combined_df = pd.concat(dataframes, ignore_index=True)
            return combined_df.to_csv(index=False)

        logging.warning("Nenhum CSV processado.")
    except Exception as e:
        logging.error(f"Erro ao processar e-mails: {e}")
        raise
