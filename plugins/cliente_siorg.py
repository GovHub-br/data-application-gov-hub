import http
from typing import Optional

from cliente_base import ClienteBase


class ClienteSiorg(ClienteBase):

    BASE_URL = "https://estruturaorganizacional.dados.gov.br/doc"

    def __init__(self) -> None:
        super().__init__(base_url=ClienteSiorg.BASE_URL)

    def get_estrutura_organizacional_resumida(
        self,
        codigo_poder: Optional[str] = None,
        codigo_esfera: Optional[str] = None,
        codigo_unidade: Optional[str] = None,
    ) -> Optional[list]:
        """
        Consultar Estrutura Organizacional Resumida.

        Args:
            codigo_poder (Optional[str]): código do poder
            codigo_esfera (Optional[str]): código da esfera
            codigo_unidade (Optional[str]): código da unidade

        Returns:
            dict: Estrutura Organizacional Resumida.
        """
        endpoint = "/estrutura-organizacional/resumida"
        params = {}
        if codigo_poder:
            params["codigoPoder"] = codigo_poder
        if codigo_esfera:
            params["codigoEsfera"] = codigo_esfera
        if codigo_unidade:
            params["codigoUnidade"] = codigo_unidade

        status, data = self.request(http.HTTPMethod.GET, endpoint, params=params)
        return (
            data.get("unidades", [])
            if status == http.HTTPStatus.OK and type(data) is dict
            else None
        )

    def get_estrutura_organizacional_cargos(
        self,
        codigo_unidade: Optional[str] = None,
    ) -> Optional[dict]:
        """
        Consultar Estrutura Organizacional Cargos.

        Args:
            codigo_unidade (Optional[str]): código da unidade

        Returns:
            dict: Estrutura Organizacional Cargos.
        """
        endpoint = "/instancias/consulta-unidade"
        params = {}
        if codigo_unidade:
            params["codigoUnidade"] = codigo_unidade

        headers = {"accept": "*/*"}

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, params=params, headers=headers
        )
        return (
            data.get("unidade", [])
            if status == http.HTTPStatus.OK and type(data) is dict
            else None
        )

    def get_cargos_funcao(self) -> Optional[dict]:
        """
        Consultar Cargos Função.

        Returns:
            dict: Cargos Função.
        """
        endpoint = "/cargo-funcao"

        status, data = self.request(http.HTTPMethod.GET, endpoint)
        return (
            data.get("tipoCargoFuncao", [])
            if status == http.HTTPStatus.OK and type(data) is dict
            else None
        )
