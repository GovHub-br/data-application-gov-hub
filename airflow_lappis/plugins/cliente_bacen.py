from typing import Optional, Tuple
from http import HTTPStatus

from cliente_base import ClienteBase


class ClienteBacen(ClienteBase):
    def __init__(
        self, base_url: str = "https://api.bcb.gov.br", headers: Optional[dict] = None
    ) -> None:
        super().__init__(base_url, headers)

    def get_serie(
        self,
        codigo_serie: int,
        data_inicial: Optional[str] = None,
        data_final: Optional[str] = None,
    ) -> Tuple[HTTPStatus, Optional[dict | list]]:
        path = f"/dados/serie/bcdata.sgs.{codigo_serie}/dados"
        params = {"formato": "json"}

        if data_inicial:
            params["dataInicial"] = data_inicial
        if data_final:
            params["dataFinal"] = data_final

        return self.request("GET", path, params=params)
