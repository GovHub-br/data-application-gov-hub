"""
Cliente para interagir com a API do GitHub.
"""

import base64
import logging
from typing import Dict, Any, Optional

import requests


class ClienteGitHub:
    """Cliente para operações com GitHub API."""

    BASE_URL = "https://api.github.com"

    def __init__(self, token: str) -> None:
        """
        Inicializa o cliente GitHub.

        Args:
            token: Token de acesso pessoal do GitHub
        """
        self.token = token
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
        }
        logging.info("[cliente_github.py] Cliente GitHub inicializado")

    def get_file_sha(
        self, owner: str, repo: str, path: str, branch: str = "main"
    ) -> Optional[str]:
        """
        Obtém o SHA de um arquivo no repositório.

        Args:
            owner: Proprietário do repositório
            repo: Nome do repositório
            path: Caminho do arquivo no repositório
            branch: Branch (padrão: main)

        Returns:
            SHA do arquivo ou None se não existir
        """
        url = f"{self.BASE_URL}/repos/{owner}/{repo}/contents/{path}"
        params = {"ref": branch}

        try:
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code == 200:
                data = response.json()
                sha: Optional[str] = data.get("sha")
                logging.info(f"[cliente_github.py] SHA obtido para {path}: {sha}")
                return sha
            elif response.status_code == 404:
                logging.info(f"[cliente_github.py] Arquivo {path} não existe ainda")
                return None
            else:
                logging.error(
                    f"[cliente_github.py] Erro ao obter SHA: {response.status_code} - "
                    f"{response.text}"
                )
                return None
        except Exception as e:
            logging.error(f"[cliente_github.py] Erro ao obter SHA: {str(e)}")
            return None

    def update_file(
        self,
        owner: str,
        repo: str,
        path: str,
        content: str,
        message: str,
        branch: str = "main",
    ) -> Dict[str, Any]:
        """
        Cria ou atualiza um arquivo no repositório.

        Args:
            owner: Proprietário do repositório
            repo: Nome do repositório
            path: Caminho do arquivo no repositório
            content: Conteúdo do arquivo (será codificado em base64)
            message: Mensagem do commit
            branch: Branch (padrão: main)

        Returns:
            Resposta da API do GitHub

        Raises:
            Exception: Se houver erro na atualização
        """
        url = f"{self.BASE_URL}/repos/{owner}/{repo}/contents/{path}"

        # Codificar conteúdo em base64
        content_bytes = content.encode("utf-8")
        content_base64 = base64.b64encode(content_bytes).decode("utf-8")

        # Obter SHA do arquivo existente (necessário para atualização)
        sha = self.get_file_sha(owner, repo, path, branch)

        # Preparar payload
        payload = {
            "message": message,
            "content": content_base64,
            "branch": branch,
        }

        if sha:
            payload["sha"] = sha
            logging.info(f"[cliente_github.py] Atualizando arquivo existente: {path}")
        else:
            logging.info(f"[cliente_github.py] Criando novo arquivo: {path}")

        try:
            response = requests.put(url, headers=self.headers, json=payload)

            if response.status_code in [200, 201]:
                data: Dict[str, Any] = response.json()
                logging.info(
                    f"[cliente_github.py] Arquivo {path} "
                    f"{'atualizado' if sha else 'criado'} com sucesso"
                )
                return data
            else:
                error_msg = (
                    f"Erro ao atualizar arquivo: {response.status_code} - "
                    f"{response.text}"
                )
                logging.error(f"[cliente_github.py] {error_msg}")
                raise Exception(error_msg)

        except Exception as e:
            logging.error(f"[cliente_github.py] Erro ao atualizar arquivo: {str(e)}")
            raise
