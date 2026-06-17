---
name: govhub-domain-guide
description: >
  Guia de domínio do Gov Hub BR — o que são os sistemas, instrumentos e documentos
  financeiros do governo federal que este projeto modela. Use quando o desenvolvedor
  perguntar "o que é X?", "qual a diferença entre X e Y?", "de onde vêm esses dados?",
  ou quando estiver modelando dados de qualquer sistema governamental pela primeira vez.
---

# Gov Hub — Guia de Domínio

Este guia responde as dúvidas de domínio que surgem ao trabalhar com dados do governo federal.
**Não é um guia técnico de pipeline** — para isso, use `govhub-pipeline-guide`.

---

## Os Sistemas-Fonte

Cada sistema governamental expõe dados via API e alimenta uma ou mais fontes no projeto.

| Sistema | O que é | Schema no Postgres |
|---|---|---|
| **SIAFI** | Sistema contábil oficial do governo federal. Fonte autoritativa para empenhos, notas de crédito, programação financeira e ordens bancárias. | `siafi` |
| **TransfereGov** | Sistema de gestão de transferências (TEDs, convênios, termos de fomento). Centraliza planos de trabalho, relatórios e monitoramento. | `transfere_gov` |
| **Tesouro Gerencial** | Visão analítica do SIAFI — exporta relatórios de execução orçamentária e financeira em formato tabular. Usado para NE, NC, PF filtradas por UG/órgão. | `tesouro_gerencial` |
| **ComprasGov** | Sistema de contratos e compras do governo federal. Empenhos aqui são os mesmos do SIAFI, vinculados a contratos. | `compras_gov` |
| **SIAPE** | Sistema de gestão de pessoas (servidores, aposentados, pensionistas). | `siape` |
| **SIORG** | Estrutura organizacional do governo federal (unidades, cargos, funções). | `siorg` |
| **SICONV** | Sistema de convênios (legado do TransfereGov). Ainda alimenta dados históricos. | `siconv` |
| **PNCP** | Portal Nacional de Contratações Públicas — licitações e contratações. | `pncp` |
| **IBGE** | Dados socioeconômicos (mulheres, quilombolas, etc.) para cruzamentos. | `ibge` |
| **Dados Abertos** | Dados do portal de dados abertos (deputados, senadores, partidos). | `dados_abertos` |

---

## Os Instrumentos de Transferência

São os "contratos" que formalizam como dinheiro público se move entre partes.

### TED — Termo de Execução Descentralizada
- **Entre quem:** órgãos e entidades **federais** (ex: Ministério A → Universidade Federal B)
- **Lógica:** a unidade descentralizadora tem o recurso mas não a expertise; a descentralizada executa
- **Sistemas:** formalizado no SIAFI (número de registro), monitorado via TransfereGov
- **Documentos gerados no SIAFI:** NC (Nota de Movimentação de Crédito) + PF (Nota de Programação Financeira) — ambas obrigatoriamente vinculadas ao número do TED
- **Vínculo com NE:** a Nota de Empenho referencia o TED em campos de texto livre (`ne_ccor_descricao`, `doc_observacao`, `ne_info_complementar`) — não há campo estruturado

### Convênio
- **Entre quem:** governo federal (concedente) + ente subnacional ou ONG (convenente)
- **Diferencial:** exige contrapartida do convenente; prestação de contas formal
- **Sistema:** TransfereGov (todo o ciclo); empenho registrado no SIAFI

### Termo de Fomento
- **Entre quem:** governo federal + OSC (organização da sociedade civil)
- **Diferencial:** o projeto é **proposto pela OSC**, não pelo governo
- **Sistema:** TransfereGov

### Emenda Pix (Transferência Especial)
- **Entre quem:** União → estado/município (emenda parlamentar)
- **Diferencial:** sem convênio, sem contrato de repasse; dinheiro passa a pertencer ao ente imediatamente
- **Sistema:** SIAFI (empenho/liquidação/pagamento); rastreabilidade via Siafic local do ente

---

## Os Documentos Financeiros

São os registros contábeis gerados durante a execução.

### NE — Nota de Empenho
- Compromisso orçamentário: reserva de verba para uma finalidade específica
- Primeira etapa da despesa pública (empenho → liquidação → pagamento)
- **Fonte autoritativa: SIAFI**
- Aparece também no ComprasGov (vinculada a contratos) e no Tesouro Gerencial (exportação analítica) — é o mesmo documento, caminhos diferentes
- Campos-chave: `ne_ccor` (código completo), `ne` (últimos 12 dígitos), `orgao_id` (6 primeiros dígitos), `ne_ccor_descricao` (texto livre onde o TED é mencionado)

### NC — Nota de Movimentação de Crédito
- Registra a descentralização de créditos orçamentários de uma UG para outra
- Usada no TED para transferir o teto orçamentário
- Campo `nc_transferencia` contém o número do TED

### PF — Nota de Programação Financeira
- Registra a liberação do recurso financeiro (dinheiro em caixa)
- Complementar à NC no fluxo do TED

### OB — Ordem Bancária
- Pagamento efetivo ao credor final
- Etapa final da despesa (após empenho e liquidação)

---

## Hierarquia TED → PA → NE

```
Programa (TransfereGov)
  └── Plano de Ação / PA (TransfereGov)
        └── num_transf → identifica o TED no SIAFI
              └── NE (Nota de Empenho no SIAFI)
                    └── NC + PF (movimentação de crédito e financeira)
```

**O vínculo NE ↔ TED não é estruturado.** O número do TED (`num_transf`) precisa ser extraído por regex de campos de texto livre da NE. O pipeline usa até 9 métodos de extração em cascata — isso não é bug, é reflexo da realidade do SIAFI.

---

## Os Parceiros e seus Escopos

O projeto tem dois projetos dbt, cada um com escopo de parceiro:

| Projeto dbt | Parceiro | Foco |
|---|---|---|
| `ipea` | IPEA (Instituto de Pesquisa Econômica Aplicada) | Contratos, servidores, orçamento geral, TEDs sem filtro de órgão |
| `mir` | MIR (Ministério da Igualdade Racial) | TEDs onde MIR é descentralizadora **ou** descentralizada; emendas; convênios do MIR |

Quando um novo parceiro for onboardado, um novo projeto dbt deve ser criado com seu escopo específico.

---

## Perguntas Frequentes

**"Esse empenho veio do ComprasGov ou do SIAFI? São diferentes?"**
São o mesmo documento (NE). O ComprasGov os expõe vinculados a contratos; o SIAFI é a fonte contábil autoritativa. Se houver conflito de valor, confie no SIAFI.

**"O que é `num_transf`?"**
O número identificador de um TED no SIAFI. Formato: 6 dígitos (ex: `123456`) ou `1XXXXX`. Extraído por regex da descrição da NE.

**"O que é `ptres`?"**
Programa de Trabalho Resumido — código orçamentário que identifica a ação governamental. Não é um sistema, é um campo de classificação orçamentária.

**"O que é UG / UO?"**
- **UG (Unidade Gestora):** unidade que executa o orçamento (código de 6 dígitos, ex: `200999`)
- **UO (Unidade Orçamentária):** agrupamento de UGs por órgão/ministério

**"O que é PTRES vs. Ação?"**
PTRES identifica o programa de trabalho dentro de um órgão; Ação (`acao_governo`) é o item orçamentário dentro do programa. São hierárquicos: Ação → PTRES.
