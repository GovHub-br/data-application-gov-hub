import logging
from typing import List, Dict, Any, Optional
from cliente_base import ClienteBase

class ClienteAlphaVantage(ClienteBase):
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
 
        super().__init__(base_url="https://www.alphavantage.co")

    def get_daily_series(self, symbol: str) -> List[Dict[str, Any]]:
        """
        Busca a série temporal diária e formata para inserção no banco.
        """
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": self.api_key
        }
        
        logging.info(f"[AlphaVantage] Buscando dados para {symbol}...")
        status, dados = self.request("GET", "/query", params=params)
        
        if not dados or "Time Series (Daily)" not in dados:
            logging.error(f"Erro ao buscar dados ou limite de API atingido: {dados}")
            return []

        series = dados["Time Series (Daily)"]
        dados_formatados = []


        for data_pregao, valores in series.items():
            registro = {
                "symbol": symbol,
                "data_pregao": data_pregao,
                "open": float(valores["1. open"]),
                "high": float(valores["2. high"]),
                "low": float(valores["3. low"]),
                "close": float(valores["4. close"]),
                "volume": int(valores["5. volume"])
            }
            dados_formatados.append(registro)
            
        return dados_formatados