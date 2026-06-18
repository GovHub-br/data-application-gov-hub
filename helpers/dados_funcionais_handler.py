"""
Handler para processamento de dados funcionais do SIAPE.
Responsável por filtrar e selecionar registros funcionais ativos.
"""

import logging
from typing import Dict, List
import xml.etree.ElementTree as ET


class DadosFuncionaisHandler:
    """Handler para processar dados funcionais do SIAPE."""

    @staticmethod
    def extract_dados_funcionais_elements(xml_string: str) -> List[ET.Element]:
        """Extrai elementos DadosFuncionais do XML."""
        ns = {
            "soap": "http://schemas.xmlsoap.org/soap/envelope/",
            "soapenv": "http://schemas.xmlsoap.org/soap/envelope/",
        }
        root = ET.fromstring(xml_string)

        # Busca o body usando ambos os namespaces possíveis
        body = root.find("soap:Body", ns) or root.find("soapenv:Body", ns)
        if body is None:
            logging.warning("SOAP Body não encontrado no XML")
            return []

        # Busca todos os elementos DadosFuncionais usando iter() que ignora namespaces
        dados_funcionais_items = []
        for elem in body.iter():
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            if tag == "DadosFuncionais":
                dados_funcionais_items.append(elem)

        return dados_funcionais_items

    @staticmethod
    def convert_elements_to_registros(
        elementos: List[ET.Element],
    ) -> List[Dict[str, str | None]]:
        """Converte elementos XML em dicionários de registros."""
        registros = []
        for item in elementos:
            registro = {}
            for elem in item:
                tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
                registro[tag] = (
                    elem.text.strip() if elem.text and elem.text.strip() else None
                )
            registros.append(registro)
        return registros

    @staticmethod
    def select_best_registro(
        registros: List[Dict[str, str | None]],
    ) -> Dict[str, str | None]:
        """Seleciona o melhor registro funcional baseado nas regras de negócio."""
        if not registros:
            return {}

        # Filtra registros ativos (sem dataOcorrExclusao ou com valor vazio/None)
        registros_ativos = [
            r
            for r in registros
            if not r.get("dataOcorrExclusao") or r.get("dataOcorrExclusao") == ""
        ]

        logging.info(f"Registros ativos (sem dataOcorrExclusao): {len(registros_ativos)}")

        if not registros_ativos:
            return DadosFuncionaisHandler._handle_no_active_records(registros)

        if len(registros_ativos) == 1:
            return DadosFuncionaisHandler._handle_single_active_record(
                registros_ativos[0]
            )

        return DadosFuncionaisHandler._handle_multiple_active_records(registros_ativos)

    @staticmethod
    def _handle_no_active_records(
        registros: List[Dict[str, str | None]],
    ) -> Dict[str, str | None]:
        """Lida com o caso onde não há registros ativos."""
        logging.warning(
            "Nenhum registro funcional ativo encontrado (todos têm dataOcorrExclusao)"
        )
        # Se não há registros ativos, retorna o mais recente
        # baseado em dataIngressoFuncao
        registros_com_data = [r for r in registros if r.get("dataIngressoFuncao")]
        if registros_com_data:
            registros_ordenados = sorted(
                registros_com_data,
                key=lambda x: x.get("dataIngressoFuncao") or "00000000",
                reverse=True,
            )
            data_ingresso = registros_ordenados[0].get("dataIngressoFuncao")
            logging.info(
                f"Retornando registro mais recente: "
                f"dataIngressoFuncao={data_ingresso}"
            )
            return registros_ordenados[0]
        else:
            # Se não há datas de ingresso, retorna o primeiro
            return registros[0] if registros else {}

    @staticmethod
    def _handle_single_active_record(
        registro: Dict[str, str | None],
    ) -> Dict[str, str | None]:
        """Lida com o caso onde há apenas um registro ativo."""
        matricula = registro.get("matriculaSiape")
        logging.info(f"Retornando único registro ativo: matricula={matricula}")
        return registro

    @staticmethod
    def _handle_multiple_active_records(
        registros_ativos: List[Dict[str, str | None]],
    ) -> Dict[str, str | None]:
        """Lida com o caso onde há múltiplos registros ativos."""
        # Se há múltiplos registros ativos, retorna o mais recente
        # baseado em dataIngressoFuncao
        registros_ativos_com_data = [
            r for r in registros_ativos if r.get("dataIngressoFuncao")
        ]

        if registros_ativos_com_data:
            registro_mais_recente = max(
                registros_ativos_com_data,
                key=lambda x: x.get("dataIngressoFuncao") or "00000000",
            )
        else:
            # Se nenhum tem data de ingresso, pega o primeiro ativo
            registro_mais_recente = registros_ativos[0]

        logging.info(
            f"Múltiplos registros ativos encontrados, selecionando o mais recente: "
            f"dataIngressoFuncao={registro_mais_recente.get('dataIngressoFuncao', 'N/A')}, "  # noqa: E501
            f"matricula={registro_mais_recente.get('matriculaSiape', 'N/A')}"
        )
        return registro_mais_recente
