# Dashboards

Este diretório contém DAGs responsáveis por gerar arquivos JSON com dados consolidados para dashboards e publicá-los no GitHub.

## DAGs Disponíveis

### `dashboard_servidores_dag.py`

Gera dados do dashboard de servidores públicos e publica no GitHub.

**Agenda**: Diariamente às 6h da manhã

**Tasks**:
1. `generate_dashboard_json` - Busca dados do banco e gera estrutura JSON (retorna via XCom)
2. `publish_to_github` - Recebe os dados via XCom e publica no repositório GitHub

**Saída**: 
- GitHub: `https://github.com/davi-aguiar-vieira/IA-Portfolio/blob/main/docs/land/public/data/pessoas_visao_geral.json`

**Nota**: Os dados são passados entre tasks via XCom, sem necessidade de armazenamento local.

**Estrutura do JSON gerado**:

```json
{
  "meta": {
    "atualizado_em": "2025-11-16T06:00:00Z"
  },
  "kpis": {
    "total_servidores": 1138,
    "servidores_ativos_permanentes": 303,
    "aposentados": 703,
    "estagiarios": 15,
    "terceirizados": 623
  },
  "genero": {
    "feminino_percent": 25.1,
    "masculino_percent": 74.9
  },
  "raca_cor": [
    { "nome_cor": "BRANCA", "valor": 250 },
    { "nome_cor": "PARDA", "valor": 72 }
  ],
  "situacao_funcional": [
    { "label": "Aposentado", "valor": 703 },
    { "label": "Ativo permanente", "valor": 303 }
  ]
}
```

**Fontes de dados**:

- `pessoas.kpis_servidores` - KPIs consolidados
- `pessoas.distribuicao_genero` - Distribuição por gênero
- `pessoas.distribuicao_raca_cor` - Distribuição por raça/cor
- `pessoas.distribuicao_situacao_funcional` - Distribuição por situação funcional

Todos os modelos são gerenciados pelo dbt no diretório `airflow_lappis/dags/dbt/ipea/models/pessoas_dbt/gold/`.

## Configuração Necessária

### Variáveis do Airflow

A DAG requer as seguintes variáveis configuradas no Airflow:

- **`GITHUB_TOKEN`**: Token de acesso pessoal do GitHub com permissões de escrita no repositório

Para configurar via CLI:

```bash
airflow variables set GITHUB_TOKEN "seu_token_aqui"
```

Ou via interface web: **Admin** → **Variables** → **+**

### Conexão Postgres

A DAG utiliza a conexão `postgres_default` do Airflow. Configure-a apontando para:

- **Host**: `postgres`
- **Schema**: `analytics`
- **Login**: `analytics`
- **Password**: `analytics`
- **Port**: `5432`

## Executando Manualmente

Para executar a DAG manualmente via CLI:

```bash
airflow dags trigger dashboard_servidores_json
```

## Logs

Os logs da execução podem ser consultados através da interface do Airflow ou via CLI:

```bash
airflow tasks logs dashboard_servidores_json generate_dashboard_json <execution_date>
```
