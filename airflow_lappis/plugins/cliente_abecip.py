from typing import Optional, Tuple, Dict, Any
from http import HTTPStatus

from cliente_base import ClienteBase


class ClienteAbecip(ClienteBase):
    def __init__(
        self, base_url: str = "https://api.abecip.org.br", headers: Optional[dict] = None
    ) -> None:
        super().__init__(base_url, headers)

    def get_financiamentos(
        self,
        ano: int,
        mes: Optional[int] = None,
    ) -> Tuple[HTTPStatus, Optional[Dict[str, Any] | list]]:
        path = "/dados/financiamentos"
        params = {"ano": ano}

        if mes is not None:
            params["mes"] = mes

        return self.request("GET", path, params=params)

    def get_indicadores(self) -> Tuple[HTTPStatus, Optional[Dict[str, Any] | list]]:
        path = "/dados/indicadores"
        return self.request("GET", path)
