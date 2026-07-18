# Meta 3 — Cotas de ações afirmativas (domínio `cotas_dbt`)

> Estado do pipeline dbt que audita o cumprimento das cotas de ações afirmativas
> (raça/cor, PCD, indígena e território) da **LPG** (Lei Paulo Gustavo) e da **PNAB**
> (Política Nacional Aldir Blanc). Documento vivo — foco em **cobertura de dados**.
>
> Schema de saída: `cotas` · Branch: `feature/cotas-meta3` · Atualizado: 15/07/2026

---

## 1. Visão geral

O domínio `cotas_dbt` segue a arquitetura **medalhão** (bronze → silver → gold) e cruza
dois lados de dados:

- **Lado-valor** — quem recebeu quanto (listas de contemplados; e, para PNAB, o extrato
  bancário bbágil quando disponível).
- **Lado-perfil** — raça/cor, PCD, indígena de cada agente (fichas demográficas).

O grão final (`fct_pagamentos_elegiveis`) é **1 linha por pagamento** (payment-first):
todo pagamento é preservado, mesmo sem perfil ou sem ano, para o denominador das cotas
ser fiel e os "órfãos" (sem perfil) ficarem visíveis.

As cotas são reportadas por ano, em **valor ponderado**, com veredito vs meta:

| Grupo | Meta mínima | LPG | PNAB |
|---|---|---|---|
| Negra | 25% | ✅ | ✅ |
| Indígena | 10% | ✅ | ✅ |
| PCD | 5% | ✅ | ✅ |
| Território vulnerabilizado | 20% | ✅ | — (só LPG) |

---

## 2. O que está FEITO

- **Cotas LPG completas** — as 4 cotas (negra, indígena, PCD, território 20%) calculadas
  em `distribuicao_cotas_lpg`.
- **Cotas PNAB (via listas)** — 3 cotas (negra, indígena, PCD) em `distribuicao_cotas_pnab`.
  Golds **separadas por programa** (LPG e PNAB não se misturam), via a macro
  `distribuicao_cotas(programa, incluir_territorio)`.
- **Datação de ano melhorada** — de **56,2% → 61,6%** de cobertura, com a 3ª camada
  `nome_arquivo` (`edital_ano_por_arquivo`). +28.950 pagamentos datados.
- **Território (4ª cota LPG)** — crosswalk IBGE de territórios periféricos ingerido
  (`territorio_fcu_setores`, 33k setores), colapsado a município (`territorio_municipio`)
  e cruzado com o agente por nome cidade+UF.
- **Limpeza de lixo de planilha** — subtotais ("TOTAL DE RECURSOS") e instruções de
  template filtrados em `contemplados_unif` para não vazarem ao fato.
- **Rastreio** — `anexo_id` e `nome_arquivo` propagados até o fato.

## 3. O que COMEÇOU a ser implementado

- **bbágil (valor pago real, PNAB)** — 4 modelos prontos (`stg_bbagil`, `bbagil_ente_ano`,
  `fct_pagamentos_bbagil`, `comparativo_recebido_vs_pago`), **desabilitados**
  (`{{ config(enabled=false) }}`) até a extração gerar `bsc_pnab.fato_bbagil`.
  Bloqueio atual: a API do BSC (`bsc.cultura.gov.br`) só resolve via **VPN do governo**.
- **Ingestão de território** — DAG `ingest_territorio_fcu_dag` criada; carrega o CSV IBGE
  no Postgres. Já rodou (33.273 linhas).

## 4. O que SERÁ implementado (melhora de cobertura)

1. **Ativar o bbágil** quando a extração rodar (ou via dump do ambiente autorizado):
   trocar `enabled=false → true` nos 4 modelos + reativar no `gold/schema.yml`.
2. **Trocar o denominador PNAB** de valor-recebido (listas, ~R$2,7bi) para valor-pago
   (bbágil, ~R$447mi). Diagnóstico 4: o fato via listas mede **repasse a entes**, não
   pagamento a pessoas — o correto para cota é o pago.
3. **Querido Diário** — datação por diário oficial (ano + IBGE do município). Planejado;
   o IBGE vem do bbágil (`ente_bbagil = id_plano_acao` → `raw_planos_acao.codigo_ibge`).
4. **Refinar o casamento territorial** por nome (~47,5% dos com-perfil casam hoje;
   grafias divergentes de município subestimam a cota de 20%).
5. **Melhorar a cobertura de perfil** — backfill de mais fichas PNAB. Hoje é teto de dado
   (nem todo contemplado preencheu ficha de raça/PCD).

---

## 5. Camadas dbt (foco em cobertura de dados)

### Bronze (`+materialized: table`) — staging, filtra o lixo de parsing
| Modelo | Papel na cobertura |
|---|---|
| `stg_agentes_pf` / `stg_agentes_pj` / `stg_agentes_coletivos` | Perfil LPG (raça/PCD + cidade/uf). Chave = CPF/CNPJ normalizado. |
| `stg_agentes_pnab_pf` / `stg_agentes_pnab_pj` | Perfil PNAB (CPF/CNPJ **real**, não mascarado). |
| `stg_contemplados_lpg` | Lado-valor LPG. `coalesce_por_nome` resolve schema-drift. |
| `stg_contemplados_pnab` | Lado-valor PNAB (geral + PNCV). CPF anonimizado vira flag. |
| `stg_editais` | Editais/derivação de ano. |
| `stg_bbagil` (disabled) | Lado-valor PNAB via banco (valor pago). |

### Silver (`+materialized: view`) — normalização, datação, território
| Modelo | Papel na cobertura |
|---|---|
| `perfil_agentes_normalizado` | Unifica os 5 perfis, dedup por documento, flags de cota + cidade/uf + chave territorial. |
| `contemplados_unif` | Núcleo do lado-valor; datação em 3 camadas; filtra lixo. |
| `edital_ano_por_anexo` | Ano por nº do edital (abas de definição PNAB). |
| `edital_ano_por_arquivo` | **3ª camada** de ano (nome do xlsx). +849 anexos. |
| `territorio_municipio` | Crosswalk periférico a nível de município (chave por nome+UF). |
| `bbagil_ente_ano` (disabled) | Ano/IBGE do bbágil via plano de ação. |
| `editais_unif`, `identificadores_agentes_cotas`, `diag_valores_cortados` | Reconciliação e auditoria. |

### Gold (`+materialized: table`) — fato e relatórios
| Modelo | Papel |
|---|---|
| `fct_pagamentos_elegiveis` | Fato: 1 linha/pagamento + flags de cota + território + ano. |
| `cobertura_pagamentos` | **Teto de confiabilidade** por ano (ler ANTES das cotas). |
| `distribuicao_cotas_lpg` | Cotas SOMENTE LPG (4 grupos). |
| `distribuicao_cotas_pnab` | Cotas SOMENTE PNAB (3 grupos). |
| `fct_pagamentos_bbagil`, `comparativo_recebido_vs_pago` (disabled) | Fato bbágil + validação recebido vs pago. |

---

## 6. Raws consumidas (sources)

Schema `transferegov_fundo_a_fundo`:
- **Perfil LPG:** `lpg_dados_pessoa_fisica` (+ variantes audiovisual/multicultural), `_juridica`, `_coletivos`/`_grupo_coletivo`.
- **Perfil PNAB:** `pnab_pessoas`, `pnab_organizacoes`.
- **Contemplados:** `lpg_contemplados`, `raw_pnab_lista_contemplados_geral`, `raw_pnab_lista_contemplados_pncv`.
- **Editais/ano:** `lpg_editais`, `lpg_dados_instrumentos*`, `raw_pnab_acoes_gerais`, `raw_pnab_acoes_cultura_viva`.
- **Território:** `territorio_fcu_setores` (IBGE CD2022, periféricos).
- **Apoio bbágil:** `raw_programas`, `raw_planos_acao`, `raw_planos_acao_dado_bancario`, `relatorios_gestao`, `anexos_relatorios`.

Schema `bsc_pnab`: `fato_bbagil` (gerado pela extração bbágil; consumido pelos modelos disabled).

---

## 7. Como rodar

```bash
cd dbt/minc
export DBT_PROFILES_DIR="$PWD" DB_DW_HOST=localhost DB_DW_USER=postgres_dw \
       DB_DW_PASSWORD=postgres_dw DB_DW_DBNAME=data_warehouse DB_DW_SCHEMA=minc
dbt run --threads 1 --select cotas_dbt    # SEMPRE --threads 1 (senão OOM no Docker)
dbt test --select cotas_dbt
```

O Airflow/Docker: rodar `docker compose` **de dentro de `infra/`** (é onde o `.env` é lido).
Segredos reais ficam em `infra/.env` (git-ignored); `local.env` é template sem segredos.

## 8. Macros de apoio
`normaliza_documento` (CPF/CNPJ → dígitos), `sem_acento`, `parse_valor` (teto R$10M),
`coalesce_por_nome` (schema-drift), `ano_edital` (ano de texto, [2013,2026]),
`distribuicao_cotas` (gera a distribuição por programa).
