import unicodedata
import re

def normalize_column_name(col: str) -> str:

    col = unicodedata.normalize('NFKD', col).encode('ascii', 'ignore').decode('ascii')
    col = col.lower()
    col = re.sub(r'[^a-z0-9]+', '_', col)
    col = re.sub(r'_+', '_', col)
    col = col.strip('_')

    return col
