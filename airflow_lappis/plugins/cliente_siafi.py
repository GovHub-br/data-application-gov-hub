import os
import logging
from zeep import Client
from zeep.transports import Transport
from zeep.wsse.username import UsernameToken
from requests import Session
from typing import Dict, Any, Optional
from retry_helpers import retry_on_exception
import requests
import base64

# Configuração do logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ClienteSiafi:
    def __init__(self) -> None:
        """
        Inicializa o cliente SIAFI com as configurações necessárias.
        """
        self.base_url = "https://servicos-siafi.tesouro.gov.br/siafi"
        self.cert_path = os.getenv("SIAFI_CERT")
        self.key_path = os.getenv("SIAFI_KEY")
        self.siafi_username = os.getenv("SIAFI_USERNAME")
        self.siafi_password = os.getenv("SIAFI_PASSWORD")

    def _criar_cliente_soap(self, ano: int, endpoint: str) -> Optional[Client]:
        """
        Cria e retorna um cliente SOAP para comunicação com o serviço SIAFI
        com a URL específica para o ano e endpoint informados.

        Args:
            ano (int): Ano para formar a URL do WSDL
            endpoint (str): Endpoint específico do serviço SIAFI

        Returns:
            Client: Cliente SOAP configurado ou None em caso de falha.
        """
        wsdl_url = f"{self.base_url}{ano}/{endpoint}?wsdl"
        logger.info(f"Criando cliente SOAP com URL: {wsdl_url}")

        if not isinstance(self.cert_path, str) or not isinstance(self.key_path, str):
            logger.error(
                f"Certificados inválidos. cert={self.cert_path}, key={self.key_path}"
            )
            return None

        session = Session()
        session.verify = True  # Verificar certificados SSL
        session.cert = (self.cert_path, self.key_path)

        transport = Transport(session=session)
        wsse = UsernameToken(self.siafi_username, self.siafi_password, use_digest=False)

        try:
            client = Client(wsdl_url, transport=transport, wsse=wsse)
            logger.info(
                f"Cliente SOAP para o ano {ano} e endpoint {endpoint} criado com sucesso."
            )
            return client
        except requests.exceptions.SSLError as ssl_error:
            logger.error(
                f"Erro de SSL ao criar cliente SOAP. Verifique os certificados. "
                f"cert_path={self.cert_path}, key_path={self.key_path}, erro={ssl_error}"
            )
        except requests.exceptions.ConnectionError as conn_error:
            logger.error(
                f"Erro ao criar cliente SOAP. wsdl_url={wsdl_url}, erro={conn_error}"
            )
        except Exception as e:
            logger.error(
                f"Erro ao criar cliente SOAP para ano {ano} e endpoint {endpoint}. "
                f"wsdl_url={wsdl_url}, erro={e}"
            )
        return None

    @retry_on_exception(max_attempts=3, initial_delay=2.0, backoff_factor=2.0)
    def consultar_programacao_financeira(
        self, ug_emitente: str, ano: int, num_lista: str
    ) -> Optional[Dict[str, Any]]:
        """
        Consulta programações financeiras no SIAFI.

        Args:
            ug_emitente (str): UG emitente da programação financeira.
            ano (int): Ano da programação financeira.
            num_lista (str): Número do documento da programação financeira.

        Returns:
            dict: Resposta da consulta ou None em caso de falha.
        """
        endpoint = "services/pf/manterProgramacaoFinanceira"

        client = self._criar_cliente_soap(ano, endpoint)
        if not client:
            logger.error(
                f"Falha ao criar cliente SOAP para consultar programação financeira. "
                f"ug={ug_emitente}, ano={ano}, num_lista={num_lista}, endpoint={endpoint}"
            )
            return None

        soap_headers = {
            "cabecalhoSIAFI": {
                "nomeSistemaSIAFI": f"SIAFI{str(ano)}",
                "ug": ug_emitente,
                "bilhetador": {"nonce": "nonce123456"},
            }
        }

        try:
            logger.info(
                f"Consultando PF: UG={ug_emitente}, Ano={ano}, Documento={num_lista}"
            )
            response = client.service.pfDetalharProgramacaoFinanceira(
                _soapheaders=soap_headers,
                ano=ano,
                numeroDocumento=num_lista,
                codUgEmit=ug_emitente,
            )
            logger.info(f"Resposta recebida: {response}")

            response_dict: Dict[str, Any] = dict(response) if response else {}
            return response_dict
        except requests.exceptions.Timeout as timeout_error:
            logger.error(
                f"Timeout ao consultar programação financeira. "
                f"ug={ug_emitente}, ano={ano}, num_list={num_lista}, erro={timeout_error}"
            )
        except Exception as e:
            logger.error(
                f"Erro inesperado ao consultar programação financeira. "
                f"ug_emitente={ug_emitente}, ano={ano}, num_lista={num_lista}, erro={e}"
            )
        return None

    @retry_on_exception(max_attempts=3, initial_delay=2.0, backoff_factor=2.0)
    def consultar_nota_empenho(
        self, ug_emitente: str, ano_empenho: int, num_empenho: str
    ) -> Optional[Dict[str, Any]]:
        """
        Consulta detalhes de uma Nota de Empenho no SIAFI.

        Args:
            ug_emitente (str): UG emitente da Nota de Empenho.
            ano_empenho (int): Ano da Nota de Empenho.
            num_empenho (str): Número da Nota de Empenho.

        Returns:
            dict: Resposta da consulta ou None em caso de falha.
        """
        endpoint = "services/orcamentario/manterOrcamentario"
        client = self._criar_cliente_soap(ano_empenho, endpoint)
        if not client:
            logger.error(
                f"Falha ao criar cliente SOAP para consultar nota de empenho. "
                f"ug={ug_emitente}, ano={ano_empenho}, "
                f"numero={num_empenho}, endpoint={endpoint}"
            )
            return None

        soap_headers = {
            "cabecalhoSIAFI": {
                "nomeSistemaSIAFI": f"SIAFI{str(ano_empenho)}",
                "ug": ug_emitente,
                "bilhetador": {"nonce": "nonce123456"},
            }
        }

        parametros_consulta = {
            "ugEmitente": ug_emitente,
            "anoEmpenho": ano_empenho,
            "numEmpenho": num_empenho.zfill(6),
        }

        try:
            logger.info(
                f"Consultando NE: UG={ug_emitente}, Ano={ano_empenho}, Num={num_empenho}"
            )
            response = client.service.orcDetalharEmpenho(
                parametros_consulta, _soapheaders=soap_headers
            )
            logger.info(f"Resposta recebida: {response}")

            response_dict: Dict[str, Any] = dict(response) if response else {}
            return response_dict
        except requests.exceptions.Timeout as te:
            logger.error(
                f"Timeout ao consultar nota de empenho. "
                f"ug={ug_emitente}, ano={ano_empenho}, num={num_empenho}, erro={te}"
            )
        except Exception as e:
            logger.error(
                f"Erro inesperado ao consultar nota de empenho. "
                f"ug={ug_emitente}, ano={ano_empenho}, num={num_empenho}, erro={e}"
            )
        return None

    @retry_on_exception(max_attempts=3, initial_delay=2.0, backoff_factor=2.0)
    def get_access_token(self) -> Optional[str]:
        """
        Obtém um token de acesso usando autenticação HTTP Basic.

        Returns:
            str: Token de acesso ou None em caso de falha.
        """
        # Credenciais
        consumer_key = os.getenv("SIAFI_BEARER_KEY_SERPRO")
        consumer_secret = os.getenv("SIAFI_BEARER_SECRET_SERPRO")

        if not consumer_key or not consumer_secret:
            logger.error("Credenciais de autenticação não configuradas.")
            return None

        # Codificar as credenciais em Base64
        credentials = f"{consumer_key}:{consumer_secret}"
        encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode(
            "utf-8"
        )

        # URL para obter o token
        url = "https://gateway.apiserpro.serpro.gov.br/token"

        # Parâmetros de dados
        data = {"grant_type": "client_credentials"}

        # Cabeçalhos com a autorização codificada em Base64
        headers = {
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        try:
            # Realizar a requisição POST para obter o token
            response = requests.post(url, data=data, headers=headers)

            if response.status_code == 200:
                token = response.json().get("access_token")
                if isinstance(token, str):
                    logger.info("Token obtido com sucesso.")
                    return token
                else:
                    logger.error("Resposta não contém o campo 'access_token'.")
                    return None
            else:
                logger.error(
                    f"Erro ao obter o token: {response.status_code}, {response.text}"
                )
                return None
        except Exception as e:
            logger.error(f"Erro na requisição para obter o token: {e}")
            return None

    @retry_on_exception(max_attempts=3, initial_delay=2.0, backoff_factor=2.0)
    def consultar_nota_credito(
        self, ug: str, gestao: str, ano: str, numero: str
    ) -> Optional[Dict[str, Any]]:
        """
        Consulta a nota de crédito na API SIAFI.

        Args:
            ug (str): Unidade Gestora.
            gestao (str): Código da gestão.
            ano (str): Ano da nota.
            numero (str): Número da nota.

        Returns:
            Optional[dict]: JSON da resposta ou None em caso de erro.
        """

        cpf = os.getenv("SIAFI_CPF_SERPRO")
        ug_orgao = "113601"
        base_credential = f"{cpf}.{ug_orgao}.SIAFI"
        # Obter o x-credencial baseado no ano
        x_credencial = f"{base_credential}{ano}"
        encoded_x_credencial = base64.b64encode(x_credencial.encode("utf-8")).decode(
            "utf-8"
        )

        # URL da requisição
        BASE_URL = "https://gateway.apiserpro.serpro.gov.br/api-integra-siafi/api"
        url = f"{BASE_URL}/v2/nota-credito/{ug}/{gestao}/{ano}/{numero}"

        # Obtém o token de acesso
        token = self.get_access_token()
        if not token:
            logger.error("Não foi possível obter o token de acesso.")
            return {"error": "Falha ao obter o token de acesso."}

        # Configura os cabeçalhos da requisição
        headers = {
            "accept": "application/json",
            "x-credencial": encoded_x_credencial,
            "Authorization": f"Bearer {token}",
        }

        try:
            # Faz a requisição GET
            logger.info(
                f"Consultando NC: UG={ug} Gestão={gestao} Ano={ano} Número={numero}"
            )
            response = requests.get(url, headers=headers)

            # Verifica o status da resposta
            if response.status_code == 200:
                logger.info("Consulta realizada com sucesso.")
                response_json = response.json()
                if isinstance(response_json, dict):
                    return response_json
                else:
                    logger.error("Resposta não é um JSON válido.")
                    return None
            else:
                logger.error(
                    f"Erro na consulta: {response.status_code} - {response.text}"
                )
                return None
        except Exception as e:
            logger.error(f"Erro ao consultar a nota de crédito: {e}")
            return None
