import sys
import os
import pytest
from unittest.mock import Mock, patch
import httpx
from http import HTTPStatus

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "airflow_lappis", "plugins"))
from cliente_base import ClienteBase

@pytest.fixture
def cliente():
    return ClienteBase(base_url="https://api.example.com")

def test_request_success(cliente):
    """Testa se uma requisição bem-sucedida retorna o status e JSON corretos imediatamente."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": "sucesso"}

    with patch("httpx.Client.request", return_value=mock_response) as mock_request:
        status, data = cliente.request("GET", "/endpoint")

        mock_request.assert_called_once_with("GET", "/endpoint", timeout=cliente.DEFAULT_TIMEOUT)
        assert status == HTTPStatus.OK
        assert data == {"data": "sucesso"}

@patch("time.sleep")
def test_request_retry_success_on_second_attempt(mock_sleep, cliente):
    """Testa se o cliente tenta novamente após uma falha e retorna sucesso na segunda tentativa."""
    
    # Primeira chamada levanta erro, segunda retorna sucesso
    mock_success_response = Mock()
    mock_success_response.status_code = 200
    mock_success_response.json.return_value = {"data": "recuperado"}
    
    mock_error = httpx.HTTPError("Internal Server Error")
    
    with patch("httpx.Client.request", side_effect=[mock_error, mock_success_response]) as mock_request:
        status, data = cliente.request("GET", "/endpoint")

        assert mock_request.call_count == 2
        assert status == HTTPStatus.OK
        assert data == {"data": "recuperado"}
        mock_sleep.assert_called_once_with(0)  # attempt=0 -> 0**2 * 2 = 0

@patch("time.sleep")
def test_request_max_retries_exceeded(mock_sleep, cliente):
    """Testa se o cliente lança uma exceção quando todas as tentativas falham."""
    mock_error = httpx.HTTPError("Service Unavailable")

    with patch("httpx.Client.request", side_effect=[mock_error] * cliente.DEFAULT_MAX_RETRIES) as mock_request:
        with pytest.raises(Exception) as exc_info:
            cliente.request("GET", "/endpoint")

        assert "API failed after the maximum number of attempts!" in str(exc_info.value)
        assert mock_request.call_count == cliente.DEFAULT_MAX_RETRIES
        assert mock_sleep.call_count == cliente.DEFAULT_MAX_RETRIES - 1

def test_request_custom_timeout(cliente):
    """Testa se kwargs como timeout customizado são passados corretamente."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {}

    with patch("httpx.Client.request", return_value=mock_response) as mock_request:
        cliente.request("POST", "/endpoint", timeout=30)
        
        mock_request.assert_called_once_with("POST", "/endpoint", timeout=30)
