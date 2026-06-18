import logging
import time
import functools
from typing import Callable, TypeVar, Any, cast, Tuple

# Configuração do logger
logger = logging.getLogger(__name__)

# Definindo um tipo genérico para o decorador
T = TypeVar("T")


def retry_on_exception(
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions_to_retry: Tuple = (Exception,),
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorador que implementa um mecanismo de retry para funções que podem falhar.

    Args:
        max_attempts (int): Número máximo de tentativas
        initial_delay (float): Tempo inicial de espera entre tentativas em segundos
        backoff_factor (float): Multiplicador do tempo de espera para cada nova tentativa
        exceptions_to_retry (tuple): Exceções que devem ativar o retry

    Returns:
        Callable: Função decorada com mecanismo de retry
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception = None
            delay = initial_delay

            # Extrai informações para logging mais claro
            method_name = func.__name__

            for attempt in range(1, max_attempts + 1):
                try:
                    if attempt > 1:
                        logger.info(
                            f"Tentativa {attempt}/{max_attempts} para {method_name}"
                        )

                    return func(*args, **kwargs)

                except exceptions_to_retry as e:
                    last_exception = e

                    if attempt < max_attempts:

                        error_msg = (
                            f"Tentativa {attempt} falhou para {method_name}: {str(e)}"
                        )
                        logger.warning(error_msg)

                        logger.info(
                            f"Aguardando {delay:.2f}s antes da próxima tentativa..."
                        )
                        time.sleep(delay)
                        # Aumenta o delay para a próxima tentativa (backoff exponencial)
                        delay *= backoff_factor
                    else:
                        logger.error(
                            f"Todas as {max_attempts} tentativas falharam: {method_name}"
                        )

            # Se chegou aqui, todas as tentativas falharam
            if last_exception:
                raise last_exception

            # Este ponto nunca deveria ser alcançado, mas é necessário para tipagem
            raise RuntimeError("Erro inesperado no mecanismo de retry")

        return cast(Callable[..., T], wrapper)

    return decorator
