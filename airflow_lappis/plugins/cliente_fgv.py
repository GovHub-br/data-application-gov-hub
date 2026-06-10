from typing import Any, Dict, Optional, Tuple

from http import HTTPStatus

from cliente_base import ClienteBase


class ClienteFgv(ClienteBase):
    def __init__(
        self,
        base_url: str = "https://api.fgv.br",
        headers: Optional[dict] = None,
    ) -> None:
        super().__init__(base_url, headers)

    def get_indices(
        self,
    ) -> Tuple[HTTPStatus, Optional[Dict[str, Any] | list]]:
        path = "/indices"
        return self.request("GET", path)

    def get_serie(
        self,
        codigo_serie: str,
        data_inicio: Optional[str] = None,
        data_fim: Optional[str] = None,
    ) -> Tuple[HTTPStatus, Optional[Dict[str, Any] | list]]:
        path = "/series"
        params: Dict[str, Any] = {"codigo": codigo_serie}

        if data_inicio is not None:
            params["data_inicio"] = data_inicio

        if data_fim is not None:
            params["data_fim"] = data_fim

        return self.request("GET", path, params=params)