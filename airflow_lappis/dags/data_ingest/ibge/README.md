# Ingestão IBGE — DAGs do Airflow

Esta pasta concentra as DAGs de ingestão de pesquisas do IBGE. Os dados brutos
são persistidos no schema `ibge` do Postgres analítico e servem de base para a
camada bronze do dbt.

| DAG | Tabela destino | Fonte | Frequência da pesquisa | Schedule |
|---|---|---|---|---|
| `ibge_ingest_dag` | `ibge.<config.tabela>` | API Agregados (genérico) | varia | dinâmico |
| `ibge_pim_pf_brasil_ingest_dag` | `ibge.ibge_pim_pf_brasil` | PIM-PF Brasil / SIDRA tabela 8888 | mensal | diário |

---

## `ibge_pim_pf_brasil_ingest_dag`

### Fonte

- **Pesquisa**: Pesquisa Industrial Mensal — Produção Física (PIM-PF) Brasil
- **Portal IBGE**: <https://www.ibge.gov.br/estatisticas/economicas/industria/9294-pesquisa-industrial-mensal-producao-fisica-brasil.html>
- **SIDRA**: <https://sidra.ibge.gov.br/tabela/8888> (tabela 8888 — séries
  reformuladas em 2023, iniciadas em janeiro de 2022)
- **API utilizada**: `servicodados.ibge.gov.br/api/v3` (API de Dados Agregados,
  que é a interface programática equivalente ao SIDRA)
- **Endpoint efetivo**:
  `/agregados/8888/periodos/{periodos}/variaveis/{variaveis}?localidades=N1[1]`

### Frequência

- **Publicação no IBGE**: mensal (normalmente no início do mês subsequente).
- **Schedule da DAG**: `@daily` (default), configurável via Airflow Variable
  `dynamic_schedules`. A pesquisa é mensal, mas a DAG roda todo dia para
  capturar tanto a publicação do novo mês quanto revisões retroativas — comuns
  em séries dessazonalizadas da PIM-PF. Cada execução é idempotente porque o
  `insert_data` aplica `ON CONFLICT DO UPDATE`.

### Campos relevantes na tabela `ibge.ibge_pim_pf_brasil`

A transformação é feita pelo método estático
`ClienteIBGE.transformar_resposta()` (em `plugins/cliente_ibge.py`).

| Coluna | Tipo | Descrição |
|---|---|---|
| `variavel_id` | TEXT | Identificador da variável na SIDRA. As principais para PIM-PF são: variação mensal (%), variação acumulada no ano (%), variação acumulada em 12 meses (%) e número-índice (base: média de 2022 = 100). Para a lista completa, consulte os metadados da tabela (ver "Como descobrir IDs" abaixo). |
| `variavel_nome` | TEXT | Nome da variável. |
| `unidade` | TEXT | Unidade da variável (`%`, número-índice, etc.). |
| `localidade_id` | TEXT | `1` para Brasil (N1). |
| `localidade_nome` | TEXT | `Brasil`. |
| `classificacao_id` | TEXT | ID(s) da(s) classificação(ões). Para a PIM-PF, em geral inclui "Seções e atividades industriais" e "Grandes categorias econômicas". Se houver múltiplas classificações no mesmo registro, são concatenadas com `\|`. `0` quando o agregado não tem classificação. |
| `classificacao_nome` | TEXT | Nome(s) correspondente(s). |
| `categoria_id` | TEXT | ID(s) da(s) categoria(s). Indústria geral, indústrias extrativas, indústrias de transformação, atividades específicas etc. |
| `categoria_nome` | TEXT | Nome(s) correspondente(s). |
| `periodo` | TEXT | Período no formato `AAAAMM` (ex: `202602`). |
| `valor` | TEXT | Valor numérico armazenado como texto (a tipagem definitiva é feita no bronze do dbt). `NULL` quando o IBGE devolve `...` (dado indisponível/suprimido). |
| `dt_ingest` | TEXT | Timestamp ISO da ingestão. |

A chave primária composta é
`(variavel_id, localidade_id, periodo, classificacao_id, categoria_id)`.

### Configuração via Airflow Variable

A DAG aceita override completo do payload da requisição via a Variable
`IBGE_PIM_PF_BRASIL_CONFIG` (JSON). Defaults aplicados quando a Variable
não existe ou tem chaves ausentes:

```json
{
  "agregado": 8888,
  "variaveis": "all",
  "periodos": "-13",
  "nivel": "N1",
  "localidade": "1",
  "classificacao_id": null,
  "categoria": null
}
```

- `variaveis="all"` traz todas as variáveis disponíveis. Para reduzir volume,
  passe uma string de IDs separados por `|` (ex: `"11602|11603|11604"`).
- `periodos="-13"` traz os últimos 13 períodos. Use `"-all"` para carga
  histórica única ou um intervalo específico (`"202401|202502"`).
- `classificacao_id` / `categoria` nulos: a API devolve **todas** as
  classificações e categorias do agregado. Para restringir (ex: só "Indústria
  geral"), defina ambos os IDs.

### Como descobrir IDs de variáveis/classificações/categorias

Para refinar o recorte sem chutar IDs, consulte os metadados do agregado:

```bash
curl -s "https://servicodados.ibge.gov.br/api/v3/agregados/8888/metadados" | jq
```

O retorno inclui as listas completas de `variaveis` e `classificacoes` com
seus respectivos `id` e `nome`. Para listar localidades disponíveis:

```bash
curl -s "https://servicodados.ibge.gov.br/api/v3/agregados/8888/localidades/N1" | jq
```

> **Nota**: o agregado 8888 corresponde à série reformulada em 2023. Se o IBGE
> publicar uma nova reformulação no futuro com novo número de tabela
> (ver histórico de substituições no portal da PIM-PF), basta atualizar
> `agregado` na Variable — nenhuma mudança de código é necessária.

### Estratégia de carga incremental

A DAG é diária, mas a PIM-PF é mensal — então a maioria das execuções não
trará dados novos. O padrão é:

1. Buscar os últimos 13 períodos (`periodos="-13"`).
2. Inserir tudo via `ON CONFLICT (chave) DO UPDATE` — registros já existentes
   têm seus valores atualizados (importante para revisões retroativas que
   o IBGE eventualmente publica), e novos meses entram normalmente.
3. Resultado: idempotência total e captura automática de revisões da série
   histórica recente sem necessidade de detectar manualmente "o que mudou".

A escolha de 13 períodos (e não 3 ou 12) cobre o janelamento típico de
revisões + um mês de folga, sem pesar muito na ingestão.

---

## `ibge_ingest_dag` (genérico)

DAG histórica/genérica baseada em `IBGE_CONFIGURACOES` (Airflow Variable, lista
de configs). Usa dynamic task mapping para ingerir vários agregados em paralelo
na mesma run. Atualmente serve SINAPI e PIB Construção (ver tags). Para novos
datasets, prefira criar uma DAG dedicada seguindo o padrão de
`ibge_pim_pf_brasil_ingest_dag` — facilita ownership, observabilidade e schedule
independente.
