from unittest.mock import patch, MagicMock
from http import HTTPStatus
import httpx
import pytest

from airflow_lappis.plugins.cliente_mrv import ClienteMRV

class TestClienteMRV:
    @pytest.fixture
    def cliente_mrv(self):
        return ClienteMRV()

    @patch("httpx.Client.request")
    def test_consultar_empreendimentos_sucesso(self, mock_request, cliente_mrv):
        # Configura o mock para retornar uma resposta de sucesso
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = HTTPStatus.OK
        mock_response.json.return_value = [{"id": 1, "nome": "Residencial Arvoredo", "cidade": "São Paulo"}]
        mock_request.return_value = mock_response

        status, dados = cliente_mrv.consultar_empreendimentos(params={"cidade": "São Paulo"})

        # Verifica se a chamada foi feita corretamente
        mock_request.assert_called_once_with(
            "GET", 
            "/empreendimentos", 
            params={"cidade": "São Paulo"}, 
            timeout=cliente_mrv.DEFAULT_TIMEOUT
        )
        
        assert status == HTTPStatus.OK
        assert isinstance(dados, list)
        assert len(dados) == 1
        assert dados[0]["nome"] == "Residencial Arvoredo"

    @patch("httpx.Client.request")
    def test_consultar_empreendimentos_falha(self, mock_request, cliente_mrv):
        # Simula uma falha na requisição (ex: erro 404)
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = HTTPStatus.NOT_FOUND
        
        # raise_for_status deve levantar exceção para simular o comportamento real do httpx
        def raise_for_status_mock():
            raise httpx.HTTPStatusError("Not Found", request=MagicMock(), response=mock_response)
        
        mock_response.raise_for_status.side_effect = raise_for_status_mock
        mock_request.return_value = mock_response

        # Executa e espera que levante a exceção mapeada no ClienteBase
        with pytest.raises(Exception, match="API failed after the maximum number of attempts!"):
            cliente_mrv.consultar_empreendimentos()

        # Verifica que ocorreu as tentativas de retentativa definidas em ClienteBase
        assert mock_request.call_count == cliente_mrv.DEFAULT_MAX_RETRIES
