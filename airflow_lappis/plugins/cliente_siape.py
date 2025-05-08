import os
from typing import Dict, Any
import requests
import xml.etree.ElementTree as ET
from jinja2 import Environment, FileSystemLoader


class ClienteSiape:
    """
    Client to consume the SIAPE SOAP API using OAuth2 authentication
    and dynamic XML generation with Jinja2 templates.
    """

    BEARER_ENDPOINT = (
        "https://apigateway.conectagov.estaleiro.serpro.gov.br/oauth2/jwt-token/"
    )
    SOAP_ENDPOINT = "https://apigateway.conectagov.estaleiro.serpro.gov.br/api-consulta-siape/v1/consulta-siape"

    def __init__(self) -> None:
        """
        Initialize the SIAPE client using environment variables:
        - SIAPE_BEARER_USER
        - SIAPE_BEARER_PASSWORD
        - SIAPE_CPF_USER
        """
        self.oauth_user = os.getenv("SIAPE_BEARER_USER")
        self.oauth_password = os.getenv("SIAPE_BEARER_PASSWORD")
        self.cpf_usuario = os.getenv("SIAPE_CPF_USER")

        if not all([self.oauth_user, self.oauth_password, self.cpf_usuario]):
            raise ValueError("Variáveis de ambiente do SIAPE estão incompletas")

        token = self._get_token(self.oauth_user, self.oauth_password)
        self.headers = self._get_headers(token, self.cpf_usuario)
        base_path = os.environ["AIRFLOW_REPO_BASE"]
        templates_path = f"{base_path}/templates/siape"
        self.env = Environment(loader=FileSystemLoader(templates_path))

    @staticmethod
    def _get_token(oauth_username: str, oauth_password: str) -> str:
        """
        Gets the token for the client.

        Args:
            oauth_username (str): OAuth username.
            oauth_password (str): OAuth password.

        Returns:
            str: Access token.
        """
        data = {"grant_type": "client_credentials"}
        response = requests.post(
            ClienteSiape.BEARER_ENDPOINT,
            auth=(oauth_username, oauth_password),
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        json_response: dict[str, Any] = response.json()
        return str(json_response["access_token"])

    @staticmethod
    def _get_headers(token: str, cpf_usuario: str) -> Dict[str, str]:
        """
        Builds the headers for the client.

        Args:
            token (str): The OAuth token.

        Returns:
            Dict[str, str]: The headers.
        """
        return {
            "Authorization": f"Bearer {token}",
            "x-cpf-usuario": cpf_usuario,
            "Content-Type": "application/xml",
        }

    def render_xml(self, template_name: str, context: Dict[str, str]) -> str:
        """
        Render XML from a Jinja2 template and context.

        Args:
            template_name (str): Template filename
            (e.g. 'consultaDadosFuncionais.xml.j2').
            context (Dict[str, str]): Data to inject into the template.

        Returns:
            str: Rendered XML string.
        """
        template = self.env.get_template(template_name)
        return template.render(context)

    def enviar_soap(self, xml: str) -> str:
        """
        Send the XML payload to the SIAPE SOAP endpoint.

        Args:
            xml (str): The complete XML request.

        Returns:
            str: The raw XML response.
        """
        response = requests.post(
            ClienteSiape.SOAP_ENDPOINT, headers=self.headers, data=xml
        )
        response.raise_for_status()
        return response.text

    def call(self, template_name: str, context: Dict[str, str]) -> str:
        """
        Execute a SOAP request using a Jinja2 template and parameters.

        Args:
            template_name (str): Jinja2 template file name.
            context (Dict[str, str]): Parameters for rendering the XML.

        Returns:
            str: The raw XML response.
        """
        xml = self.render_xml(template_name, context)
        return self.enviar_soap(xml)

    @staticmethod
    def parse_xml_to_dict(xml_string: str) -> Dict[str, str]:
        """
        Parse a SOAP XML response and return a dictionary with tag names and values.

        Args:
            xml_string (str): SOAP XML response.

        Returns:
            Dict[str, str]: Flattened dictionary of XML data.
        """
        ns = {"soapenv": "http://schemas.xmlsoap.org/soap/envelope/"}
        root = ET.fromstring(xml_string)
        body = root.find("soapenv:Body", ns)
        if body is None:
            return {"error": "Missing SOAP Body"}

        response_elem = list(body)[0]
        return {
            child.tag.split("}")[-1]: child.text.strip()
            for child in response_elem.iter()
            if child.text and child.text.strip()
        }

    @staticmethod
    def parse_xml_to_list(
        xml_string: str, element_tag: str, namespaces: Dict[str, str]
    ) -> list[dict[str, str | None]]:
        """
        Generic parser for repeating XML elements (like lista servidores).

        Args:
            xml_string (str): SOAP XML response.
            element_tag (str): Tag do elemento que se repete.
            namespaces (Dict[str, str]): XML namespaces.

        Returns:
            list[dict[str, str | None]]: Lista de registros.
        """
        root = ET.fromstring(xml_string)
        body = root.find("soapenv:Body", namespaces)
        if body is None:
            return []

        response_elem = list(body)[0]
        items = response_elem.findall(f".//{element_tag}", namespaces)

        resultado = []
        for item in items:
            row = {}
            for elem in item:
                tag = elem.tag.split("}")[-1]
                row[tag] = elem.text.strip() if elem.text else None
            resultado.append(row)

        return resultado

    @staticmethod
    def parse_afastamento_historico(xml_string: str) -> list[dict[str, Any]]:
        """
        Custom parser for afastamento histórico: extrai DadosFerias e DadosOcorrencias.

        Args:
            xml_string (str): SOAP XML response.

        Returns:
            list[dict[str, str | None]]: Lista de registros combinando
            férias e ocorrências.
        """
        ns = {
            "soapenv": "http://schemas.xmlsoap.org/soap/envelope/",
            "ns2": "http://tipo.servico.wssiapenet",
        }
        root = ET.fromstring(xml_string)
        body = root.find("soapenv:Body", ns)
        if body is None:
            return []

        dados = []
        for item in body.findall(".//ns2:DadosFerias", ns):
            registro = {}
            for elem in item:
                tag = elem.tag.split("}")[-1]
                registro[tag] = elem.text.strip() if elem.text else None
            dados.append(registro)

        for item in body.findall(".//ns2:DadosOcorrencias", ns):
            registro = {}
            for elem in item:
                tag = elem.tag.split("}")[-1]
                registro[tag] = elem.text.strip() if elem.text else None
            dados.append(registro)

        return dados

    @staticmethod
    def parse_dependentes(xml_string: str) -> list[dict[str, Any]]:
        """
        Custom parser para consultaDadosDependentes: extrai dados do
        dependente e seus benefícios.

        Args:
            xml_string (str): SOAP XML response.

        Returns:
            list[dict[str, Any]]: Lista de dependentes com campo
            `arrayBeneficios` como sublista.
        """
        ns = {
            "soapenv": "http://schemas.xmlsoap.org/soap/envelope/",
            "ns2": "http://tipo.servico.wssiapenet",
        }
        root = ET.fromstring(xml_string)
        body = root.find("soapenv:Body", ns)
        if body is None:
            return []

        dependentes = []
        for item in body.findall(".//ns2:DadosDependentes", ns):
            registro: dict[str, Any] = {}
            for elem in item:
                tag = elem.tag.split("}")[-1]
                if tag == "arrayBeneficios":
                    beneficios = []
                    for b in elem:
                        beneficio = {
                            e.tag.split("}")[-1]: e.text.strip() for e in b if e.text
                        }
                        beneficios.append(beneficio)
                    registro[tag] = beneficios
                else:
                    registro[tag] = elem.text.strip() if elem.text else None
            dependentes.append(registro)

        return dependentes
