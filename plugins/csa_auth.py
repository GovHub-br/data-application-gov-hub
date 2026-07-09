"""Provedores de token Bearer para o SCA/SERPRO (autenticacao do BSC).

Duas variantes, para os dois estilos de cliente do pipeline BSC/PNAB:

- ``TokenProvider``: sincrono (httpx), usado por ``BscClient``.
- ``AsyncTokenProvider``: assincrono (aiohttp) com ``asyncio.Lock`` e
  double-check, usado por ``AsyncBscClient``. O double-check evita que N
  coroutines concorrentes (ex.: 5 tasks liberadas pelo mesmo Semaphore)
  disparem N chamadas redundantes de renovacao de token quando o cache
  expira: a primeira coroutine a adquirir o lock renova; as demais, ao
  adquirir o lock em seguida, encontram o token ja valido e nao fazem nova
  requisicao.
"""

import asyncio
import logging
import time
from typing import Optional

import aiohttp
import httpx

logger = logging.getLogger(__name__)


class TokenProvider:
    """Cliente de token sincrono para o SCA. Usado por pipelines sync
    (ex.: consulta unitaria de CNPJ)."""

    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        ttl_seconds: int,
        timeout: float = 30,
    ) -> None:
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.ttl_seconds = ttl_seconds
        self.timeout = timeout
        self._token: Optional[str] = None
        self._token_time: Optional[float] = None

    def _token_expirou(self) -> bool:
        if not self._token or not self._token_time:
            return True
        return (time.time() - self._token_time) > self.ttl_seconds

    def get_token(self) -> str:
        if not self._token_expirou():
            return str(self._token)

        logger.info(
            "[csa_auth] Solicitando novo token SCA (sync) | url=%s", self.token_url
        )
        headers = {
            "accept": "*/*",
            "clientid": self.client_id,
            "clientSecret": self.client_secret,
            "ldap": "false",
        }
        response = httpx.get(self.token_url, headers=headers, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()

        self._token = payload["accessToken"]
        self._token_time = time.time()
        logger.info("[csa_auth] Token SCA renovado (sync)")
        return str(self._token)


class AsyncTokenProvider:
    """Cliente de token assincrono para o SCA, com cache em memoria e
    double-check lock para evitar renovacoes redundantes sob concorrencia."""

    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        ttl_seconds: int,
        session: aiohttp.ClientSession,
        timeout: float = 30,
    ) -> None:
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.ttl_seconds = ttl_seconds
        self.session = session
        self.timeout = timeout
        self._token: Optional[str] = None
        self._token_time: Optional[float] = None
        self._lock = asyncio.Lock()

    def _token_expirou(self) -> bool:
        if not self._token or not self._token_time:
            return True
        return (time.time() - self._token_time) > self.ttl_seconds

    async def get_token(self) -> str:
        # Verificacao rapida sem lock (evita contencao no caminho feliz).
        if not self._token_expirou():
            return str(self._token)

        async with self._lock:
            # Double-check: outra coroutine pode ter renovado enquanto
            # esperavamos o lock.
            if not self._token_expirou():
                return str(self._token)

            logger.info(
                "[csa_auth] Solicitando novo token SCA (async) | url=%s", self.token_url
            )
            headers = {
                "accept": "*/*",
                "clientid": self.client_id,
                "clientSecret": self.client_secret,
                "ldap": "false",
            }
            async with self.session.get(
                self.token_url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as response:
                response.raise_for_status()
                payload = await response.json(content_type=None)

            self._token = payload["accessToken"]
            self._token_time = time.time()
            logger.info("[csa_auth] Token SCA renovado (async)")
            return str(self._token)
