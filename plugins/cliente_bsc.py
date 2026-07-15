"""Cliente assincrono para o Barramento de Servicos Corporativos (BSC/SERPRO).

Concentra toda a complexidade de HTTP (aiohttp), autenticacao (Bearer via
``AsyncTokenProvider``), concorrencia (``asyncio.Semaphore`` + throttle) e
resiliencia (retry exponencial) num unico lugar, para que as DAGs em
``dags/`` fiquem livres dessa logica e contenham apenas orquestracao.

Um unico cliente assincrono cobre os 8 endpoints do BSC (em vez de replicar
sync/async como no legado ``bsc-api-extractor``): toda task do Airflow que
usa este cliente jah precisa fazer a ponte TaskFlow -> asyncio via
``asyncio.run()``, entao nao ha ganho em manter uma segunda variante
sincrona -- so duplicaria a logica de retry/backoff.

Regras de resiliencia (nao alterar sem atualizar os testes):

- 401/403 (credencial invalida/sem permissao) ou 429 (rate limit): aborta a
  execucao inteira levantando ``BscRequestError`` -- nao adianta retry.
- 5xx ou erro de rede/timeout: retry exponencial (ate ``BSC_MAX_RETRIES``
  tentativas, espera ``BSC_RETRY_BACKOFF_BASE_SECONDS * 2**tentativa``).
- Outros 4xx: loga e retorna ``None`` para aquele item (nao aborta o lote).
- HTTP 400 com a mensagem "Nao existem lancamentos..." no extrato BB Agil e
  resultado de negocio (periodo sem lancamentos), nao erro -- ver
  ``is_empty_extrato_response``.
"""

import asyncio
import json
import logging
from typing import Any, Optional

import aiohttp

import config_bsc_pnab as settings
import payloads_bsc as payloads
from csa_auth import AsyncTokenProvider

logger = logging.getLogger(__name__)


class BscRequestError(Exception):
    """Erro de requisicao ao BSC com o status HTTP e o corpo da resposta,
    usados por quem chama para decidir se aborta ou segue (ex.: distinguir
    "sem lancamentos" de um erro real no extrato BB Agil)."""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response_text: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text


def is_empty_extrato_response(exc: BscRequestError) -> bool:
    """Distingue 'periodo sem lancamentos' (dado de negocio valido) de um
    erro real no extrato BB Agil."""
    return (
        exc.status_code == 400
        and bool(exc.response_text)
        and settings.EMPTY_EXTRATO_ERROR_MESSAGE in (exc.response_text or "")
    )


class AsyncBscClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        token_provider: AsyncTokenProvider,
        base_url: str = settings.SERPRO_BASE_URL,
        max_concurrent_requests: int = settings.BSC_MAX_CONCURRENT_REQUESTS,
        request_throttle_seconds: float = settings.BSC_REQUEST_THROTTLE_SECONDS,
        max_retries: int = settings.BSC_MAX_RETRIES,
        backoff_base_seconds: float = settings.BSC_RETRY_BACKOFF_BASE_SECONDS,
        timeout: float = settings.BSC_TIMEOUT,
    ) -> None:
        self.session = session
        self.token_provider = token_provider
        self.base_url = base_url.rstrip("/")
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.request_throttle_seconds = request_throttle_seconds
        self.max_retries = max_retries
        self.backoff_base_seconds = backoff_base_seconds
        self.timeout = aiohttp.ClientTimeout(total=timeout)

    async def _auth_headers(self) -> dict[str, str]:
        token = await self.token_provider.get_token()
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _deve_tentar_novamente(self, attempt: int) -> bool:
        return attempt < self.max_retries

    async def _aguardar_backoff(self, path: str, attempt: int, motivo: str) -> None:
        wait = self.backoff_base_seconds * (2**attempt)
        logger.warning(
            "[cliente_bsc] %s em %s (tentativa %d/%d), aguardando %.1fs",
            motivo,
            path,
            attempt + 1,
            self.max_retries,
            wait,
        )
        await asyncio.sleep(wait)

    async def _post(self, path: str, payload: dict[str, Any]) -> Any:
        """POST resiliente: Semaphore + throttle + retry exponencial em
        5xx/rede, abort imediato em 401/403/429, skip em outros 4xx."""
        url = f"{self.base_url}{path}"
        attempt = 0

        async with self.semaphore:
            if self.request_throttle_seconds > 0:
                await asyncio.sleep(self.request_throttle_seconds)

            while True:
                try:
                    headers = await self._auth_headers()
                    logger.info("[cliente_bsc] POST %s | tentativa=%d", path, attempt + 1)
                    async with self.session.post(
                        url, json=payload, headers=headers, timeout=self.timeout
                    ) as response:
                        status = response.status
                        text = await response.text()
                except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    if self._deve_tentar_novamente(attempt):
                        await self._aguardar_backoff(path, attempt, "Erro de rede")
                        attempt += 1
                        continue
                    logger.error(
                        "[cliente_bsc] Erro de rede em %s apos %d tentativas | erro=%s",
                        path,
                        self.max_retries,
                        str(exc)[:400],
                    )
                    raise

                if status in (401, 403):
                    raise BscRequestError(
                        "Erro de autorizacao (401/403). Execucao abortada.",
                        status_code=status,
                        response_text=text,
                    )
                if status == 429:
                    raise BscRequestError(
                        "Rate limit atingido (429). Execucao abortada.",
                        status_code=status,
                        response_text=text,
                    )
                if status >= 500:
                    if self._deve_tentar_novamente(attempt):
                        await self._aguardar_backoff(path, attempt, f"Erro {status}")
                        attempt += 1
                        continue
                    logger.error(
                        "[cliente_bsc] Erro %s em %s apos %d tentativas | body=%s",
                        status,
                        path,
                        self.max_retries,
                        text[:400],
                    )
                    raise BscRequestError(
                        f"Erro HTTP {status} apos {self.max_retries} tentativas",
                        status_code=status,
                        response_text=text,
                    )
                if status >= 400:
                    logger.error(
                        "[cliente_bsc] Erro HTTP nao retriavel | path=%s | status=%s | "
                        "body=%s",
                        path,
                        status,
                        text[:400],
                    )
                    raise BscRequestError(
                        f"Erro HTTP {status}", status_code=status, response_text=text
                    )

                return json.loads(text) if text else None

    # -- Beneficiarios (CPF/CNPJ) ------------------------------------------------

    async def cpf_list(self, cpfs: list[str]) -> Any:
        return await self._post("/api/cpf/list", payloads.build_payload_cpf_list(cpfs))

    async def cnpj_detalhe(self, cnpj: str) -> Any:
        return await self._post(
            "/api/cnpj/detalhe", payloads.build_payload_cnpj_detalhe(cnpj)
        )

    async def cnpj_basico(self, cnpj: str) -> Any:
        return await self._post(
            "/api/cnpj/basico", payloads.build_payload_cnpj_basico(cnpj)
        )

    async def cadunico_cpf(self, cpf: str) -> Any:
        return await self._post(
            "/api/cadunico/cpf", payloads.build_payload_cadunico(cpf)
        )

    async def beneficio_prestacao_continuada(self, cpf: str) -> Any:
        return await self._post(
            "/api/inss/v2/beneficio-prestacao-continuada",
            payloads.build_payload_bpc(cpf),
        )

    async def relacao_trabalhista(self, cpf: str) -> Any:
        return await self._post(
            "/api/inss/v1/relacao-trabalhista",
            payloads.build_payload_relacao_trabalhista(cpf),
        )

    # -- BB Gestao Agil (Orgao de Controle) ------------------------------------

    async def bbagil_extrato_orgao_controle(
        self, agencia: int, numero_conta: int, periodo_inicial: str, periodo_final: str
    ) -> Any:
        return await self._post(
            "/api/bb-gestao-agil/extrato-orgao-controle",
            payloads.build_payload_bbagil_extrato_controle(
                agencia, numero_conta, periodo_inicial, periodo_final
            ),
        )

    async def bbagil_extrato_sub_lancamentos_orgao_controle(
        self, agencia: str, numero_conta: str, id_transaction: str
    ) -> Any:
        return await self._post(
            "/api/bb-gestao-agil/extrato-sub-lancamentos-orgao-controle",
            payloads.build_payload_bbagil_subtransacoes(
                agencia, numero_conta, id_transaction
            ),
        )

    # -- Endpoints de saldo (Orgao de Repasse) --------------------------------
    # Sem contrapartida confirmada para "orgao-controle" no Swagger visto ate
    # agora. Nao usados pela DAG atual (extrato/subtransacoes de orgao de
    # controle) -- mantidos aqui sem validacao contra a API ate confirmar o
    # endpoint certo para consulta de saldo de conta de ente recebedor.
    async def bbagil_saldo_conta_corrente_orgao_repasse(
        self, agencia: int, numero_conta: int
    ) -> Any:
        # Path com a grafia exata do Swagger do BSC ("correte", nao
        # "corrente") -- nao "corrigir" o typo, o servidor so responde
        # nesse path literal.
        return await self._post(
            "/api/bb-gestao-agil/saldo-conta-correte-orgao-repasse",
            payloads.build_payload_bbagil_saldo_conta(agencia, numero_conta),
        )

    async def bbagil_saldo_aplicacoes_financeiras(
        self, agencia: int, numero_conta: int
    ) -> Any:
        return await self._post(
            "/api/bb-gestao-agil/saldo-aplicacoes-financeiras",
            payloads.build_payload_bbagil_saldo_conta(agencia, numero_conta),
        )


def build_async_client(session: aiohttp.ClientSession) -> AsyncBscClient:
    """Monta um ``AsyncBscClient`` pronto para uso a partir das
    configuracoes de ambiente (``config_bsc_pnab``), compartilhando a
    mesma ``session`` entre o token provider e o cliente."""
    token_provider = AsyncTokenProvider(
        token_url=settings.SCA_TOKEN_URL,
        client_id=settings.SCA_CLIENT_ID,
        client_secret=settings.SCA_CLIENT_SECRET,
        ttl_seconds=settings.SCA_TOKEN_TTL_SECONDS,
        session=session,
        timeout=settings.BSC_TIMEOUT,
    )
    return AsyncBscClient(session=session, token_provider=token_provider)
