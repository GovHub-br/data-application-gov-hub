import io
import logging
import pandas as pd
from typing import List, Dict, Any, Optional
from cliente_base import ClienteBase

class ClienteSiac(ClienteBase):
    """
    Cliente para extração de dados do PBQP-H (SiAC e SiMaC) 
    diretamente do portal de dados abertos.
    """

    def __init__(self, headers: Optional[dict] = None) -> None:
        # Define um User-Agent para evitar bloqueios por robôs simples
        if not headers:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
        super().__init__(base_url="https://dadosabertos.cidades.gov.br", headers=headers)

    def _get_download_link(self, termo_busca: str) -> str:
        """
        Busca dinamicamente o link de download do arquivo .xlsx no portal.
        O ID do pacote d1395db8-22a2-4468-978f-2d381ff6d927 refere-se ao PBQP-H.
        """
        id_pacote = "d1395db8-22a2-4468-978f-2d381ff6d927"
        logging.info(f"[ClienteSiac] Buscando recurso '{termo_busca}' no portal...")
        

        status, dados = self.request("GET", f"/api/3/action/package_show?id={id_pacote}")
        
        if dados and dados.get("success"):
            recursos = dados.get("result", {}).get("resources", [])
            for arquivo in recursos:
                nome_recurso = arquivo.get("name", "")
              
                if termo_busca.lower() in nome_recurso.lower():
                    url = arquivo.get("url")
                    logging.info(f"[ClienteSiac] Link encontrado: {url}")
                    return url
                    
        raise RuntimeError(f"Não foi possível localizar o arquivo '{termo_busca}' no portal.")

    def obter_dados(self, tipo: str) -> List[Dict[str, Any]]:
        """
        Faz o download do arquivo Excel e converte para dicionário Python.
        """
        link = self._get_download_link(termo_busca=tipo)
        
        logging.info(f"[ClienteSiac] Baixando planilha de {tipo}...")
        # Acessa o link direto do arquivo
        response = self.client.get(link, timeout=90.0)
        response.raise_for_status()
        

        df = pd.read_excel(io.BytesIO(response.content), engine='openpyxl')
        
       
        df = df.where(pd.notna(df), None)
        
        return df.to_dict(orient="records")

    def get_empresas_certificadas_siac(self) -> List[Dict[str, Any]]:
        return self.obter_dados("SiAC")

    def get_empresas_qualificadas_simac(self) -> List[Dict[str, Any]]:
        return self.obter_dados("SiMaC")