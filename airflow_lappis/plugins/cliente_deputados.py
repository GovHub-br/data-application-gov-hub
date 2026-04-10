import http
import logging
from typing import Any
from cliente_base import ClienteBase


class ClienteDeputados(ClienteBase):
    DEFAULT_TIMEOUT = 60
    """
    Cliente para consumir a API de Dados Abertos da Câmara dos Deputados.
    """

    BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
    BASE_HEADER = {"accept": "application/json"}

    def __init__(self) -> None:
        super().__init__(base_url=ClienteDeputados.BASE_URL)
        logging.info(
            "[cliente_deputados.py] Initialized ClienteDeputados with base_url: "
            f"{ClienteDeputados.BASE_URL}"
        )

    def get_deputados(self, **params: Any) -> list:
        """
        Obter lista de deputados
        """
        endpoint = "/deputados"
        logging.info(f"[cliente_deputados.py] Fetching deputados with params: {params}")

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER, params=params
        )

        if status == http.HTTPStatus.OK and isinstance(data, dict):
            deputados: list[dict[str, Any]] = data.get("dados", [])
            logging.info(
                f"[cliente_deputados.py] Successfully fetched {len(deputados)} deputados"
            )
            return deputados
        else:
            logging.warning(
                f"[cliente_deputados.py] Failed to fetch deputados with status: {status}"
            )
            return None

    def get_all_deputados(self) -> list:
        """
        Itera por todas as páginas da API e retorna a lista completa de deputados.
        """
        all_deputados = []
        pagina = 1

        while True:
            params = {"pagina": pagina, "itens": 1000, "dataInicio": "2010-01-01"}
            deputados = self.get_deputados(**params)

            if not deputados:
                break

            all_deputados.extend(deputados)

            if len(deputados) < 100:
                break

            pagina += 1

        return all_deputados
    
    def get_historico_deputados(self, id_deputado: int ) -> list:
        """
        Obtém o histórico de exercício parlamentar de um deputado
        """
        endpoint = f"/deputados/{id_deputado}/historico"
        logging.info(f"[cliente_deputados.py] Fetching histórico do deputado: {id_deputado}")

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER
        )

        if status == http.HTTPStatus.OK and isinstance(data, dict):
            historico = data.get("dados", [])
                
            for h in historico:
                h['id_deputado'] = id_deputado
                
            return historico
            
        return []   
        
    def get_historico_all_deputados(self) -> list:
        """
        Busca o histórico de todos os deputados e retorna lista unificada.
        """
        deputados = self.get_all_deputados()
        

        if not deputados:
            logging.warning("[cliente_deputados.py] Nenhum deputado encontrado")
            return []
        
        ids = list({d["id"] for d in deputados})
        logging.info(f"[cliente_deputados.py] Buscando histórico de {len(ids)} deputados")


        all_historico = []
        for id_deputado in ids:
            historico = self.get_historico_deputados(id_deputado)
            all_historico.extend(historico)

        logging.info(f"[cliente_deputados.py] Total de registros de histórico: {len(all_historico)}")
        return all_historico


