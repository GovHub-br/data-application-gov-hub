# Dicionário de dados — schema `cotas`

> Todas as tabelas/views do domínio `cotas_dbt` (Meta 3 — cotas de ações afirmativas).
> Derivado do código dbt (não do banco). Tipos marcados como *(inf.)* são inferidos do SQL.
> Convenções: **grão** = o que 1 linha representa. Chave de junção entre perfil e valor =
> `identificador_unico` (CPF/CNPJ só-dígitos, via macro `normaliza_documento`).
>
> Materialização: bronze = table · silver = view · gold = table. Atualizado: 15/07/2026.

---

## BRONZE (staging)

### `stg_agentes_pf` / `stg_agentes_pj` / `stg_agentes_coletivos` — perfil LPG
Grão: 1 linha por registro de agente LPG (PF / PJ / coletivo), base + variantes (audiovisual, multicultural).
| Coluna | Tipo | Descrição |
|---|---|---|
| identificador_unico | text | CPF (PF) / CNPJ (PJ) normalizado (só dígitos). Chave de junção. |
| tipo_proponente | text | 'pf', 'pj' ou 'coletivo'. |
| origem | text | 'lpg'. |
| raca_bruto | text | Raça/cor autodeclarada, texto cru. |
| pcd_bruto | text | Resposta PCD, texto cru. |
| indigena_bruto | text | Indígena, texto cru (NULL em pj/pf conforme fonte). |
| quilombola_bruto | text | Quilombola, texto cru. |
| cep, cidade, uf | text | Localização do agente (base do território). |
| origem_tabela | text | Nome da fonte/variante de origem. |
| nome_programa | text | 'LPG'. |

### `stg_agentes_pnab_pf` / `stg_agentes_pnab_pj` — perfil PNAB
Grão: 1 linha por agente PNAB. Igual ao LPG, mas CPF/CNPJ **real** (não mascarado) e com indígena/quilombola.
Colunas: `identificador_unico, tipo_proponente, origem('pnab'), raca_bruto, pcd_bruto, indigena_bruto, quilombola_bruto, cep, cidade, uf, origem_tabela, nome_programa`.

### `stg_contemplados_lpg` — lado-valor LPG
Grão: 1 linha por contemplado LPG (pagamento).
| Coluna | Tipo | Descrição |
|---|---|---|
| anexo_id | text | ID do anexo (planilha) de origem. |
| identificador_unico | text | CPF/CNPJ normalizado; NULL se doc mascarado. |
| chave_anonimizada | boolean | true se o doc veio mascarado (não casável). |
| valor_pago_num | numeric | Valor pago (teto R$10M via `parse_valor`). |
| valor_bruto_num | numeric | Valor sem teto (auditoria). |
| nome_edital | text | Nome do edital (texto). |
| link | text | Link de publicação do resultado. |
| nome_arquivo | text | Nome do xlsx de origem (usado p/ datar). |
| nome_programa | text | 'LPG'. |
| origem | text | 'lpg'. |

### `stg_contemplados_pnab` — lado-valor PNAB
Grão: 1 linha por contemplado PNAB (geral + PNCV). Chave = CNPJ real ou CPF real; CPF anonimizado vira flag.
Colunas: `anexo_id, identificador_unico, chave_anonimizada, valor_pago_num, valor_bruto_num, nome_edital, link, nome_arquivo, nome_programa('PNAB'), origem('pnab')`.

### `stg_editais` — editais LPG
Grão: 1 linha por edital. Deriva ano do número/nome. Fonte: `lpg_editais` + `lpg_dados_instrumentos*`.

### `stg_bbagil` — lado-valor PNAB via banco *(DESABILITADO)*
Grão: 1 linha por (ente, beneficiário) do extrato bancário. Fonte: `bsc_pnab.fato_bbagil`.
| Coluna | Tipo | Descrição |
|---|---|---|
| ente_bbagil | text | id_plano_acao do ente. |
| identificador_unico | text | CPF/CNPJ real normalizado. |
| documento_raw | text | Documento como veio. |
| chave_anonimizada | boolean | true se doc mascarado. |
| valor_pago_num | numeric | Valor pago (extrato, já filtrado). |

---

## SILVER

### `perfil_agentes_normalizado` — perfil unificado
Grão: **1 linha por documento** (dedup LPG+PNAB, prioriza quem tem raça declarada). Base do lado-perfil.
| Coluna | Tipo | Descrição |
|---|---|---|
| identificador_unico | text | Chave (único). |
| tipo_proponente | text | pf/pj/coletivo. |
| origem | text | lpg/pnab (o vencedor do dedup). |
| raca_bruto | text | Raça crua (do registro escolhido). |
| raca_normalizada | text | negra / indigena / branca / amarela / nao_declarada. |
| cidade, uf | text | Localização (só LPG; PNAB=NULL). |
| chave_municipio_uf | text | `sem_acento(cidade)\|sem_acento(uf)`; NULL se cidade/uf ausentes. Junta com `territorio_municipio`. |
| flag_negra | boolean | raca_normalizada = 'negra'. |
| flag_indigena | boolean | indígena por raça OU campo indígena. |
| is_pcd | boolean | PCD ∈ {sim,s,1,true}. |

### `contemplados_unif` — núcleo do lado-valor
Grão: 1 linha por pagamento (LPG+PNAB), pós-limpeza de lixo, com ano derivado.
| Coluna | Tipo | Descrição |
|---|---|---|
| identificador_unico | text | CPF/CNPJ normalizado (NULL se mascarado). |
| chave_anonimizada | boolean | doc mascarado. |
| valor_pago_num | numeric | Valor pago (com teto). |
| nome_edital | text | Nome do edital. |
| origem | text | lpg/pnab. |
| nome_programa | text | LPG/PNAB. |
| anexo_id, nome_arquivo | text | Rastreio à planilha-fonte. |
| ano_final | text | Ano do edital: coalesce(nome_edital, anexo, arquivo). |
| origem_ano | text | nome_edital / anexo_edital / nome_arquivo / sem_ano. |

### `edital_ano_por_anexo` / `edital_ano_por_arquivo` — datação por anexo
Grão: 1 linha por `anexo_id` (só anexos com ano único).
| Coluna | Tipo | Descrição |
|---|---|---|
| anexo_id | text | ID do anexo (único). |
| ano_edital | text | Ano derivado ([2013,2026]); do nº do edital (por_anexo) ou do nome do arquivo (por_arquivo). |

### `territorio_municipio` — crosswalk periférico por município
Grão: 1 linha por município (colapsa `territorio_fcu_setores`).
| Coluna | Tipo | Descrição |
|---|---|---|
| cd_mun | text | Código IBGE do município. |
| nm_mun | text | Nome do município. |
| cd_uf, nm_uf | text | Código/nome da UF. |
| sigla_uf | text | Sigla da UF (mapa código→sigla). |
| em_concentracao_urbana | boolean | Município tem área de concentração urbana/FCU. |
| chave_municipio_uf | text | `sem_acento(nm_mun)\|lower(sigla_uf)`. Junta com o perfil. |

### `bbagil_ente_ano` — datação/localização do bbágil *(DESABILITADO)*
Grão: 1 linha por ente (id_plano_acao). Colunas: `ente_bbagil, codigo_ibge, municipio, uf, ano_plano` (ano da vigência do plano).

### Auxiliares
- `editais_unif` — editais unificados + `ano_edital` + flag `cotas` (reconciliação).
- `identificadores_agentes_cotas` — lista de `identificador_unico` distintos (teste de unicidade).
- `diag_valores_cortados` — diagnóstico: `faixa_valor`, `qtd` (audita o teto de R$10M).

---

## GOLD

### `fct_pagamentos_elegiveis` — o fato
Grão: **1 linha por pagamento** (payment-first, LEFT JOIN perfil e território).
| Coluna | Tipo | Descrição |
|---|---|---|
| identificador_unico | text | CPF/CNPJ (NULL se mascarado). |
| valor_pago_num | numeric | Valor do pagamento. |
| ano_final | text | Ano do edital. |
| origem_ano | text | Camada que datou (nome_edital/anexo/arquivo/sem_ano). |
| origem | text | lpg/pnab. |
| chave_anonimizada | boolean | doc mascarado. |
| nome_edital | text | Nome do edital. |
| nome_programa | text | LPG/PNAB. |
| anexo_id, nome_arquivo | text | Rastreio à planilha. |
| tem_perfil | boolean | Casou com perfil (raça/PCD). |
| flag_negra / flag_indigena / flag_pcd | boolean | Pertence ao grupo de cota. |
| agente_cidade, agente_uf | text | Localização (do perfil; só LPG). |
| tem_territorio | boolean | Casou no crosswalk periférico. |
| flag_territorio_vulneravel | boolean | Território vulnerabilizado (casou no crosswalk); NULL se sem cidade. |

### `cobertura_pagamentos` — teto de confiabilidade
Grão: 1 linha por ano (`ano_final`, com 'sem_ano' para NULL). Ler ANTES das cotas.
| Coluna | Tipo | Descrição |
|---|---|---|
| ano_final | text | Ano ou 'sem_ano'. |
| qtd_pagamentos | bigint | Nº de pagamentos. |
| qtd_pessoas | bigint | Documentos distintos. |
| qtd_pessoas_com_perfil | bigint | Distintos com perfil. |
| valor_total | numeric | Valor total do ano. |
| valor_com_perfil | numeric | Valor de quem tem perfil. |
| cobertura_pessoas_pct | numeric | % pessoas com perfil. |
| cobertura_valor_pct | numeric | % valor com perfil. |
| cobertura_temporal_pct | numeric | % valor com ano (não sem_ano). |

### `distribuicao_cotas_lpg` / `distribuicao_cotas_pnab` — as cotas
Grão: 1 linha por (ano, grupo de cota). LPG tem 4 grupos (inclui território); PNAB tem 3.
| Coluna | Tipo | Descrição |
|---|---|---|
| programa | text | 'LPG' ou 'PNAB'. |
| ano_final | text | Ano do edital. |
| grupo | text | negra / indigena / pcd / territorio_vulneravel (só LPG). |
| valor_grupo | numeric | Valor pago ao grupo no ano. |
| valor_total_ano | numeric | Valor total do ano. |
| valor_total_com_perfil_ano | numeric | Valor de quem tem perfil. |
| pct_sobre_total | numeric | valor_grupo / total (%). |
| pct_sobre_com_perfil | numeric | valor_grupo / com-perfil (%) — base do veredito. |
| qtd_agentes_grupo | bigint | Agentes distintos do grupo. |
| meta_minima_pct | numeric | Meta legal: 25/10/5/20. |
| status_sobre_com_perfil | text | 'alcancada' ou 'descumprida'. |

### `fct_pagamentos_bbagil` / `comparativo_recebido_vs_pago` *(DESABILITADOS)*
- `fct_pagamentos_bbagil`: fato de pagamentos via banco × perfil (denominador correto p/ cotas PNAB). Colunas espelham `fct_pagamentos_elegiveis` + `codigo_ibge`/`municipio`/`uf`.
- `comparativo_recebido_vs_pago`: 2 linhas (recebido via listas vs pago via bbágil) com `fonte, linhas, docs_distintos, valor_total`.

---

## Sources (raws consumidas)
Schema `transferegov_fundo_a_fundo`: perfil LPG (`lpg_dados_pessoa_fisica`+variantes, `_juridica`, `_coletivos`/`_grupo_coletivo`), perfil PNAB (`pnab_pessoas`, `pnab_organizacoes`), contemplados (`lpg_contemplados`, `raw_pnab_lista_contemplados_geral`, `raw_pnab_lista_contemplados_pncv`), editais (`lpg_editais`, `lpg_dados_instrumentos*`, `raw_pnab_acoes_gerais`, `raw_pnab_acoes_cultura_viva`), território (`territorio_fcu_setores`), apoio bbágil (`raw_programas`, `raw_planos_acao`, `raw_planos_acao_dado_bancario`, `relatorios_gestao`, `anexos_relatorios`).
Schema `bsc_pnab`: `fato_bbagil`.
