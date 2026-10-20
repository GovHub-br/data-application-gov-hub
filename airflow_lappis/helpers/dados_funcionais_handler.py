"""
Handler para processamento de dados funcionais do SIAPE.
Responsável por filtrar e selecionar registros funcionais ativos.
"""

import logging
from datetime import date, datetime
from typing import Dict, List
import xml.etree.ElementTree as ET

# Campos registrados no log quando o SIAPE devolve mais de um vínculo por CPF.
# Comparados em minúsculas para não depender da grafia exata das tags.
CAMPOS_LOG_VINCULO = (
    "matriculasiape",
    "nomesitfuncional",
    "siglaorgao",
    "siglauorgexercicio",
    "dataingressofuncao",
    "dataocorringressoorgao",
    "dataocorrexclusao",
)

# codOcorrExclusao de "ERRO DE CADASTRAMENTO OU DUPLICIDADE": o registro foi
# lançado por engano e não representa um vínculo real.
COD_EXCLUSAO_ERRO_CADASTRAMENTO = "02027"


class DadosFuncionaisHandler:
    """Handler para processar dados funcionais do SIAPE."""

    @staticmethod
    def _parse_data_siape(valor: str | None) -> date | None:
        """Converte uma data do SIAPE no formato DDMMYYYY. Retorna None se inválida."""
        if not valor:
            return None
        try:
            return datetime.strptime(valor.strip(), "%d%m%Y").date()
        except ValueError:
            return None

    @staticmethod
    def _chave_data_ingresso(registro: Dict[str, str | None]) -> date:
        """Chave de ordenação por dataIngressoFuncao; sem data válida fica por último."""
        data = DadosFuncionaisHandler._parse_data_siape(
            registro.get("dataIngressoFuncao")
        )
        return data or date.min

    @staticmethod
    def _chave_data_exclusao(registro: Dict[str, str | None]) -> date:
        """Chave de ordenação por dataOcorrExclusao; sem data válida fica por último."""
        data = DadosFuncionaisHandler._parse_data_siape(registro.get("dataOcorrExclusao"))
        return data or date.min

    @staticmethod
    def _log_vinculos(registros: List[Dict[str, str | None]]) -> None:
        """Registra no log todos os vínculos devolvidos para o mesmo CPF."""
        logging.info(f"SIAPE devolveu {len(registros)} vínculos para o mesmo CPF:")
        for i, registro in enumerate(registros, start=1):
            campos = {
                chave: valor
                for chave, valor in registro.items()
                if chave.lower() in CAMPOS_LOG_VINCULO
            }
            logging.info(f"  vínculo {i}: {campos}")

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

        if len(registros) > 1:
            DadosFuncionaisHandler._log_vinculos(registros)

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
        """Lida com o caso onde não há registros ativos.

        Retorna o vínculo encerrado mais recentemente (maior dataOcorrExclusao),
        que é a última situação do servidor no órgão. Registros excluídos por erro
        de cadastramento são ignorados, a menos que sejam os únicos.
        """
        logging.warning(
            "Nenhum registro funcional ativo encontrado (todos têm dataOcorrExclusao)"
        )
        registros_validos = [
            r
            for r in registros
            if r.get("codOcorrExclusao") != COD_EXCLUSAO_ERRO_CADASTRAMENTO
        ] or registros

        registro_mais_recente = max(
            registros_validos, key=DadosFuncionaisHandler._chave_data_exclusao
        )
        logging.info(
            f"Retornando vínculo encerrado mais recente: "
            f"dataOcorrExclusao={registro_mais_recente.get('dataOcorrExclusao')}, "
            f"matricula={registro_mais_recente.get('matriculaSiape')}"
        )
        return registro_mais_recente

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
            r
            for r in registros_ativos
            if DadosFuncionaisHandler._parse_data_siape(r.get("dataIngressoFuncao"))
        ]

        if registros_ativos_com_data:
            registro_mais_recente = max(
                registros_ativos_com_data,
                key=DadosFuncionaisHandler._chave_data_ingresso,
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
