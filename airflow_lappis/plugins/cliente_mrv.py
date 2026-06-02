from airflow_lappis.plugins.cliente_base import ClienteBase
from typing import Optional, Dict, Any, Tuple
from http import HTTPStatus

class ClienteMRV(ClienteBase):
    """
    Cliente para integração com a API da MRV.
    """
    def __init__(self, base_url: str = "https://api.mrv.com.br", headers: Optional[dict] = None) -> None:
        super().__init__(base_url=base_url, headers=headers)

    def consultar_empreendimentos(self, params: Optional[Dict[str, Any]] = None) -> Tuple[HTTPStatus, Optional[dict | list]]:
        """
        Consulta a lista de empreendimentos imobiliários da MRV.
        """
        return self.request("GET", "/empreendimentos", params=params)
