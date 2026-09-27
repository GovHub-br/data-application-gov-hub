# Mapeamento de linhagem dos dashboards para camada Gold

## Objetivo

Mapear os gráficos e painéis atuais que consomem dados sem uma tabela Gold claramente definida e propor a modelagem Gold necessária para centralizar as regras de negócio no dbt.

## Escopo analisado

- Repositório alvo: `data-application-gov-hub`
  - DAG versionada de exportação: `airflow_lappis/dags/dashboards/dashboard_servidores_dag.py`
  - Consultas de dashboard: `airflow_lappis/plugins/cliente_postgres.py`
  - Modelos dbt: `airflow_lappis/dags/dbt/ipea/models`
- Front-end e documentação consultados em `../gov-hub`
  - `docs/dashboards/index.html`
  - `docs/dashboards/dashboards.js`
  - `docs/acompanhamento-orcamentario/index.html`
  - `docs/acompanhamento-orcamentario/acompanhamento-orcamentario.js`
  - `docs/dashboard-visao-geral-de-pessoas/index.html`
  - `docs/dashboard-visao-geral-de-pessoas/dashboard-pessoas.js`
  - `docs/land/public/data/*.json`

## Sumário executivo

1. O dashboard `Dashboard de visão geral de pessoal`, gerado pela DAG `dashboard_servidores_json`, já consome modelos Gold no schema `pessoas`.
2. A página `Visão Geral IPEA` consome vários JSONs públicos em `gov-hub/docs/land/public/data`. O repositório alvo não possui DAG/exportador versionado para a maior parte desses arquivos, portanto a linhagem atual não é auditável ponta a ponta.
3. O domínio `orcamento` é o principal ponto de melhoria: os modelos disponíveis para os gráficos atuais estão em Bronze/Silver (`visao_orcamentaria_total`, `orcamento_total_`, `categoria_gastos_orcamento_total_`) e não há Gold específica para os cards e gráficos publicados.
4. Contratos e TEDs possuem Golds de base, mas faltam views/tabelas Gold com a granularidade exata dos JSONs do dashboard, reduzindo a chance de consultas ad hoc sobre Silver/Bronze.
5. A seção legada de Pessoas na página `Visão Geral IPEA` usa JSONs separados (`servidores.json`, `servidores_sexo.json`, `servidores_cor.json`) enquanto o dashboard novo já usa `pessoas_visao_geral.json`. Recomenda-se consolidar esse consumo nos Golds existentes.

## Matriz de linhagem De -> Para

| Dashboard/painel | Gráfico ou card | Arquivo/consulta atual | Origem atual identificada | Camada atual | Gold proposta |
|---|---|---|---|---|---|
| Visão Geral IPEA | Orçamento total, orçamento recebido de TEDs, dotação atualizada, orçamento empenhado, a liquidar, despesas a pagar, despesas pagas | `visao_orcamentaria_total_ipea.json` | Não há exportador versionado no repo. Payload é compatível com agregações de `orcamento.visao_orcamentaria_total` e regras de `orcamento_total_`/`categoria_gastos_orcamento_total_` | Bronze/Silver inferido | `orcamento.dashboard_resumo_anual` |
| Visão Geral IPEA | Distribuição de orçamento por ação | `orcamento_por_acao.json` | Não há exportador versionado. Campos derivam de `acao_governo_desc` e `dotacao_atualizada` de `visao_orcamentaria_total` | Bronze/Silver inferido | `orcamento.dashboard_orcamento_por_acao` |
| Visão Geral IPEA | Como o dinheiro está sendo gasto? Orçamento e gasto por elemento de despesa | `orcamento_por_elemento_despesa.json` | Não há exportador versionado. Métricas existem em `categoria_gastos_orcamento_total_` e/ou `visao_orcamentaria_total` | Silver/Bronze inferido | `orcamento.dashboard_execucao_por_elemento_despesa` |
| Visão Geral IPEA | Quantos contratos vigentes o IPEA tem? | `contratos.json` | Exportador não encontrado. Pode ser atendido por agregação de `contratos.contratos_resumo` | Não auditável; Gold existente provável | `contratos.dashboard_contratos_por_categoria` |
| Visão Geral IPEA | Quanto do orçamento está alocado para contratos? | `orcamento_contratos.json` | Exportador não encontrado. Pode ser atendido por `contratos.contratos_somatorio` | Não auditável; Gold existente provável | `contratos.dashboard_orcamento_contratos` |
| Visão Geral IPEA | 10 maiores contratos pelo orçamento alocado e natureza da despesa | `10_maiores_contratos_natureza_despesa.json` | Exportador não encontrado. A regra exige fornecedor + natureza da despesa, hoje presente em Silver (`contratos_empenhos`) e dimensões de contrato | Silver inferido | `contratos.dashboard_top_fornecedores_natureza_despesa` |
| Visão Geral IPEA | Quais TEDs o IPEA recebeu? KPIs | `teds_recebidos.json` | Exportador não encontrado. Pode ser atendido por `ted.resumo_programa_plano_acao_`/`ted.ted_resumo_orcamentario` | Não auditável; Gold existente parcial | `ted.dashboard_teds_resumo` |
| Visão Geral IPEA | Quais TEDs o IPEA recebeu? tabela | `detalhamento_teds_recebidos.json` | Exportador não encontrado. Pode ser atendido por `ted.resumo_programa_plano_acao_` | Não auditável; Gold existente parcial | `ted.dashboard_teds_detalhamento` |
| Visão Geral IPEA | Quais TEDs o IPEA enviou? KPIs | `teds_enviados.json` | Exportador não encontrado. Pode ser atendido por `ted.resumo_programa_plano_acao_` filtrando emitente | Não auditável; Gold existente parcial | `ted.dashboard_teds_resumo` |
| Visão Geral IPEA | Quais TEDs o IPEA enviou? tabela | `detalhamento_teds_enviados.json` | Exportador não encontrado. Pode ser atendido por `ted.resumo_programa_plano_acao_` filtrando emitente | Não auditável; Gold existente parcial | `ted.dashboard_teds_detalhamento` |
| Visão Geral IPEA | Servidores ativos, estagiários, terceirizados, aposentados | `servidores.json` | Exportador não encontrado. Há Gold equivalente em `pessoas.kpis_servidores` | Não auditável; Gold existente | Usar `pessoas.kpis_servidores` ou `pessoas.dashboard_visao_geral` |
| Visão Geral IPEA | Distribuição por gênero | `servidores_sexo.json` | Exportador não encontrado. Há Gold equivalente em `pessoas.distribuicao_genero` | Não auditável; Gold existente | Usar `pessoas.distribuicao_genero` |
| Visão Geral IPEA | Distribuição por raça/cor | `servidores_cor.json` | Exportador não encontrado. Há Gold equivalente em `pessoas.distribuicao_raca_cor` | Não auditável; Gold existente | Usar `pessoas.distribuicao_raca_cor` |
| Visão Geral IPEA | Aposentadorias - total e série mensal | `servidores.json` + série mock `MOCK_APOSENTADORIAS_SERIE` | Total vem de JSON legado; série mensal está hardcoded no JS | Sem camada de dados | `pessoas.dashboard_aposentadorias_mensal` |
| Acompanhamento orçamentário | Dados do contrato, indicadores Tesouro e ComprasNet | `acompanhamento_orcamentario.json` | Exportador não encontrado. Golds `contratos_resumo`, `contratos_somatorio` e `contratos_comparativo_mensal` atendem parcialmente | Não auditável; Gold existente parcial | `contratos.dashboard_acompanhamento_orcamentario` |
| Acompanhamento orçamentário | Histórico Tesouro Gerencial | `acompanhamento_orcamentario.json.historico.tesouro` | Exportador não encontrado. Regra existe em `contratos_comparativo_mensal.siafi_valor_pago` | Gold existente parcial | `contratos.dashboard_acompanhamento_orcamentario_mensal` |
| Acompanhamento orçamentário | Histórico ComprasNet planejado x faturado | `acompanhamento_orcamentario.json.historico.comprasNet*` | Exportador não encontrado. Regra existe em `contratos_comparativo_mensal.comprasgov_*` | Gold existente parcial | `contratos.dashboard_acompanhamento_orcamentario_mensal` |
| Dashboard de visão geral de pessoal | KPIs de servidores | `ClientPostgresDB.get_dashboard_kpis()` | `pessoas.kpis_servidores` | Gold | Sem nova Gold obrigatória |
| Dashboard de visão geral de pessoal | Por gênero | `ClientPostgresDB.get_dashboard_genero()` | `pessoas.distribuicao_genero` | Gold | Sem nova Gold obrigatória |
| Dashboard de visão geral de pessoal | Por raça/cor | `ClientPostgresDB.get_dashboard_raca_cor()` | `pessoas.distribuicao_raca_cor` | Gold | Sem nova Gold obrigatória |
| Dashboard de visão geral de pessoal | Distribuição por situação funcional | `ClientPostgresDB.get_dashboard_situacao_funcional()` | `pessoas.distribuicao_situacao_funcional` | Gold | Sem nova Gold obrigatória |
| Dashboard de visão geral de pessoal | Distribuição por localidade | `ClientPostgresDB.get_dashboard_mapa_uf()` | `pessoas.distribuicao_mapa_uf` | Gold | Sem nova Gold obrigatória |
| Dashboard de visão geral de pessoal | Tabela resumo de servidores | `ClientPostgresDB.get_dashboard_tabela_servidores()` | `pessoas.tabela_servidores_agregada` | Gold | Sem nova Gold obrigatória |

Observação: os painéis `Perfil`, `Escolaridade`, `Terceirizados` e `Estagiários` do dashboard de Pessoas estão como placeholders no HTML e não consomem dados atualmente.

## Especificação técnica das novas Golds

### 1. `orcamento.dashboard_resumo_anual`

- Finalidade: atender os cards de orçamento da página `Visão Geral IPEA`.
- Materialização: `table`.
- Granularidade: uma linha por `ano_referencia`.
- Fontes recomendadas:
  - `ref('categoria_gastos_orcamento_total_')` para métricas por categoria financeira.
  - `ref('orcamento_total_')` para validação do orçamento total consolidado.
  - Se a regra de TEDs não estiver integralmente disponível em Silver, criar antes uma Silver auxiliar para separar orçamento próprio e orçamento recebido por TEDs.
- Colunas:
  - `ano_referencia`
  - `dotacao_atualizada`
  - `orcamento_recebido_teds`
  - `orcamento_total`
  - `orcamento_empenhado`
  - `orcamento_a_liquidar`
  - `despesas_a_pagar`
  - `despesas_pagas`
  - `dt_ingest`
- Regras:
  - `orcamento_total = dotacao_atualizada + orcamento_recebido_teds`.
  - `orcamento_a_liquidar` deve representar o saldo empenhado ainda não liquidado.
  - Todos os valores monetários devem ser `numeric(15,2)` ou maior.
- Testes mínimos:
  - `not_null`: `ano_referencia`, `orcamento_total`, `dt_ingest`.
  - `unique`: `ano_referencia`.
  - teste customizado: `orcamento_total >= dotacao_atualizada`.

### 2. `orcamento.dashboard_orcamento_por_acao`

- Finalidade: gráfico de rosca `Distribuição de orçamento por ação`.
- Materialização: `table`.
- Granularidade: `ano_referencia`, `acao_governo`, `acao_governo_desc`.
- Fonte recomendada: `ref('visao_orcamentaria_total')` ou, preferencialmente, uma Silver já agregada por ação.
- Colunas:
  - `ano_referencia`
  - `acao_governo`
  - `descricao`
  - `valor`
  - `percentual_orcamento`
  - `rank_valor`
  - `dt_ingest`
- Regras:
  - `valor = sum(dotacao_atualizada)`.
  - `percentual_orcamento = valor / sum(valor) over (partition by ano_referencia)`.
  - Ordenar por `rank_valor` no exportador, não no front-end.
- Testes mínimos:
  - `not_null`: `ano_referencia`, `acao_governo`, `descricao`, `valor`.
  - `accepted_range`: `valor >= 0`.

### 3. `orcamento.dashboard_execucao_por_elemento_despesa`

- Finalidade: cartões de rosca `Como o dinheiro está sendo gasto?`.
- Materialização: `table`.
- Granularidade: `ano_referencia`, `elemento_despesa`, `elemento_despesa_desc`.
- Fonte recomendada: `ref('categoria_gastos_orcamento_total_')`.
- Colunas:
  - `ano_referencia`
  - `elemento_despesa`
  - `elemento_despesa_desc`
  - `dotacao`
  - `orcamento_alocado_empenhado`
  - `despesas_programadas_a_pagar`
  - `despesas_pagas`
  - `percentual_empenhado`
  - `percentual_pago`
  - `rank_dotacao`
  - `dt_ingest`
- Regras:
  - Pivotar `categoria` em colunas de métrica.
  - `percentual_empenhado = orcamento_alocado_empenhado / nullif(dotacao, 0)`.
  - `percentual_pago = despesas_pagas / nullif(dotacao, 0)`.
  - Manter `[A DETALHAR]` como categoria explícita, sem regra no front-end.
- Testes mínimos:
  - `not_null`: `ano_referencia`, `elemento_despesa_desc`, `dt_ingest`.
  - `accepted_range`: percentuais entre `0` e `2` para permitir sobre-execução controlada.

### 4. `contratos.dashboard_contratos_por_categoria`

- Finalidade: gráfico `Quantos contratos vigentes o IPEA tem?`.
- Materialização: `view` ou `table`.
- Granularidade: `ano_referencia`, `categoria`.
- Fonte recomendada: `ref('contratos_resumo')`.
- Colunas:
  - `ano_referencia`
  - `categoria`
  - `quantidade_contratos`
  - `percentual_contratos`
  - `dt_ingest`
- Regras:
  - Considerar contrato vigente quando `situacao = 'Ativo'` ou quando `current_date between vigencia_inicio and vigencia_fim`; a regra deve ser escolhida e documentada.
  - Padronizar categorias nulas como `Não informado`.

### 5. `contratos.dashboard_orcamento_contratos`

- Finalidade: cards `Orçamento alocado para contratos`.
- Materialização: `table`.
- Granularidade: `ano_referencia`.
- Fonte recomendada: `ref('contratos_somatorio')`.
- Colunas:
  - `ano_referencia`
  - `orcamento_alocado_empenhado`
  - `saldo_de_empenho_a_liquidar`
  - `despesas_pagas`
  - `dt_ingest`
- Regras:
  - `orcamento_alocado_empenhado = sum(total_empenhado)`.
  - `despesas_pagas = sum(total_pago)`.
  - `saldo_de_empenho_a_liquidar = sum(total_empenhado - total_pago)`.

### 6. `contratos.dashboard_top_fornecedores_natureza_despesa`

- Finalidade: gráfico `10 maiores contratos pelo orçamento alocado e natureza da despesa`.
- Materialização: `table`.
- Granularidade: `ano_referencia`, `fornecedor_nome`, `natureza_despesa`.
- Fontes recomendadas:
  - `ref('contratos_empenhos')` para natureza da despesa e valores empenhados.
  - `ref('contratos_resumo')` para fornecedor e atributos contratuais.
- Colunas:
  - `ano_referencia`
  - `fornecedor_nome`
  - `natureza_despesa`
  - `natureza_despesa_descricao`
  - `valor_empenhado`
  - `rank_fornecedor`
  - `dt_ingest`
- Regras:
  - Agregar por fornecedor e natureza.
  - O top 10 deve ser calculado na Gold via `rank_fornecedor`, não no JavaScript.
  - A exportação pode pivotar naturezas para manter compatibilidade com o JSON atual, mas a Gold deve permanecer em formato longo.

### 7. `contratos.dashboard_acompanhamento_orcamentario`

- Finalidade: dados do contrato e indicadores consolidados da página `Acompanhamento orçamentário`.
- Materialização: `table`.
- Granularidade: `contrato_id`.
- Fontes recomendadas:
  - `ref('contratos_resumo')`
  - `ref('contratos_somatorio')`
- Colunas:
  - `contrato_id`
  - `numero`
  - `categoria`
  - `fornecedor_nome`
  - `objeto`
  - `valor_contratado`
  - `valor_parcela`
  - `numero_parcelas`
  - `valor_global`
  - `tesouro_valor_empenhado`
  - `tesouro_valor_a_liquidar`
  - `tesouro_valor_pago`
  - `comprasgov_valor_cronogramas`
  - `comprasgov_valor_faturado`
  - `comprasgov_valor_contratual_disponivel`
  - `dt_ingest`

### 8. `contratos.dashboard_acompanhamento_orcamentario_mensal`

- Finalidade: linhas históricas `Tesouro Gerencial` e `ComprasNet`.
- Materialização: `table`.
- Granularidade: `contrato_id`, `mes_ref`.
- Fonte recomendada: `ref('contratos_comparativo_mensal')`.
- Colunas:
  - `contrato_id`
  - `mes_ref`
  - `mes_label`
  - `siafi_valor_pago`
  - `comprasgov_valor_pago`
  - `comprasgov_valor_cronograma`
  - `dt_ingest`
- Regras:
  - `comprasgov_valor_pago` deve vir de faturas pagas, não de valor total faturado.
  - Preencher meses sem movimento com zero para o gráfico não precisar aplicar fallback.

### 9. `ted.dashboard_teds_resumo`

- Finalidade: KPIs de TEDs recebidos e enviados.
- Materialização: `table`.
- Granularidade: `ano_referencia`, `tipo_fluxo` (`recebido` ou `enviado`).
- Fonte recomendada: `ref('resumo_programa_plano_acao_')`.
- Colunas:
  - `ano_referencia`
  - `tipo_fluxo`
  - `quantidade_teds`
  - `quantidade_teds_proximos_finalizar`
  - `valor_firmado`
  - `destaque_orcamentario`
  - `despesas_a_liquidar_teds`
  - `dt_ingest`
- Regras:
  - `tipo_fluxo = 'recebido'` quando `ted_beneficiario_emitente = 'beneficiario'`.
  - `tipo_fluxo = 'enviado'` quando `ted_beneficiario_emitente = 'emitente'`.
  - `quantidade_teds_proximos_finalizar` deve ser parametrizada por ano de referência, não hardcoded para 2025.

### 10. `ted.dashboard_teds_detalhamento`

- Finalidade: tabelas de TEDs recebidos e enviados.
- Materialização: `table`.
- Granularidade: `plano_acao`, `num_transf`.
- Fonte recomendada: `ref('resumo_programa_plano_acao_')`.
- Colunas:
  - `tipo_fluxo`
  - `plano_acao`
  - `num_transf`
  - `programa`
  - `unidade`
  - `vigencia`
  - `dt_inicio_vigencia`
  - `dt_fim_vigencia`
  - `valor_firmado`
  - `percentual_conclusao`
  - `percentual_conclusao_orcamentaria`
  - `dt_ingest`
- Regras:
  - `vigencia` deve ser campo formatado apenas para compatibilidade com o JSON; manter datas normalizadas.
  - Percentuais devem ficar entre `0` e `1`.

### 11. `pessoas.dashboard_aposentadorias_mensal`

- Finalidade: substituir a série hardcoded `MOCK_APOSENTADORIAS_SERIE` do gráfico `Quantos se aposentam nos próximos anos?`.
- Materialização: `table`.
- Granularidade: `ano_referencia`, `mes_referencia`.
- Fonte recomendada: `ref('aposentadorias_resumo')`.
- Colunas:
  - `ano_referencia`
  - `mes_referencia`
  - `quantidade_aposentadorias`
  - `dt_ingest`
- Regras:
  - Para aposentadorias realizadas: agrupar por `mes_aposentadoria`.
  - Para projeções futuras, criar regra explícita com a área de negócio antes da implementação.

## Priorização sugerida

| Prioridade | Item | Motivo |
|---|---|---|
| P0 | Criar Golds de orçamento (`dashboard_resumo_anual`, `dashboard_orcamento_por_acao`, `dashboard_execucao_por_elemento_despesa`) | Hoje os gráficos mais pesados parecem depender de Bronze/Silver e não há Gold para consumo analítico. |
| P1 | Versionar exportadores dos JSONs públicos no repo de dados | Sem DAG/exportador, a linhagem do front-end não é auditável. |
| P1 | Criar Golds de acompanhamento de contratos | Evita consultas diretas sobre comparativos intermediários e padroniza o contrato exibido. |
| P2 | Criar Golds de compatibilidade para Contratos/TEDs | As Golds base existem, mas faltam tabelas prontas no formato dos painéis. |
| P2 | Consolidar JSONs legados de Pessoas | Reduz divergência entre `servidores*.json` e `pessoas_visao_geral.json`. |
| P3 | Substituir série mock de aposentadorias | Remove dado estático do front-end. |

## Observações para a issue futura de implementação

- Confirmar os nomes físicos dos modelos de orçamento. Os arquivos possuem sufixo `_` (`orcamento_total_.sql`, `categoria_gastos_orcamento_total_.sql`), enquanto o `schema.yml` documenta nomes sem esse sufixo. Isso deve ser corrigido ou tratado antes de referenciar essas models em novas Golds.
- Evitar que exportadores JSON consultem `raw`, `bronze` ou `silver` diretamente. A regra deve ser: front-end/JSON/Superset consulta apenas Gold.
- Onde o JSON atual exige formato específico, manter a Gold normalizada e fazer apenas a transformação de formato no exportador.
- Incluir `dt_ingest` em todas as Golds para permitir mostrar atualização e rastrear recência.
