# ADR 0004 — A revisão semanal apura por script e narra por modelo

- **Data:** 2026-08-13
- **Status:** aceito

## Contexto

A proposta de organização previa um relatório semanal automático — issues
fechadas na semana e progresso das abertas — como insumo para o acompanhamento
das metas. A equipe pediu que fosse uma skill: além de contar, ela deveria olhar
os commits da semana na `main` e nas outras branches, os PRs, e dizer **o que foi
feito e o que não foi feito**.

Isso é mais do que um relatório determinístico consegue: dizer o que foi feito
exige ler o diff e o corpo da issue, e traduzir para o vocabulário de quem
acompanha meta.

## Decisão

Uma skill, `revisao-semanal`, dividida em duas metades com responsabilidades
separadas:

1. **`scripts/apurar.sh`** coleta os fatos — commits, branches, PRs, issues,
   vocabulário de labels. Só coleta. Não interpreta, não resume, não conta
   história.
2. **O `SKILL.md`** manda o modelo ler essa saída, abrir os diffs e as issues
   para descrever a substância, cruzar as fontes, e escrever.

O modelo **não pode escrever número que não esteja na saída do script**. Se o
número não existe lá, ou ele obtém com um comando adicional, ou registra no
relatório que não foi possível apurar. Estimar está proibido.

A skill é acionada à mão, não por cron.

## Por quê

Relatório de acompanhamento que superestima é pior que relatório nenhum, porque a
coordenação decide em cima dele. E o jeito de errar é sempre o mesmo: contar de
cabeça. Separar as duas metades torna o erro estruturalmente difícil — o número
ou está na saída do script, ou não entra.

É o mesmo princípio que o repositório irmão adotou na `atualizar-docs-pages`
("nunca digite um número no template") e que a `accountability-report` do
GovHub-skills já segue: os fatos vêm da fonte, a narrativa é escrita, e quando um
número não pode ser apurado com confiança, o relatório diz isso em vez de estimar.

Rodar à mão, e não por cron, por quatro razões: a skill é o mesmo artefato nos
dois casos, então nada se perde ao adiar; ajustar o prompt é muito mais rápido
interativamente do que por rodada de CI; o cron exigiria instalar o Claude GitHub
App e criar um secret que o repositório não tem, o que depende de permissão de
admin; e com duas issues abertas o relatório ainda sairia magro demais para
julgar se vale automatizar.

## Alternativas descartadas

**Relatório determinístico puro**, sem modelo. Nunca erra número, mas só consegue
dizer "fecharam 4 issues" — que não permite decidir nada. Não atende ao pedido de
dizer o que foi feito.

**Os dois em paralelo** — o workflow determinístico da proposta original mais a
skill. Descartado: os dois leem a mesma fonte, e dois relatórios semanais
divergindo é pior que um.

## Consequências

O que a skill pode afirmar sobre "o que não foi feito" é limitado ao observável:
*"a #10 está aberta desde 08/06 e não teve commit nem comentário no período"* —
e não *"a #10 está atrasada"*, porque a skill não conhece o que foi combinado.
Essa fronteira está escrita no `SKILL.md`, com exemplo do certo e do errado.

Enquanto as labels `meta-*` não existirem, o relatório sai sem agrupamento por
meta e registra isso — ver [ADR 0003](0003-labels-por-eixo.md).

Se um dia o cron for ligado, o workflow é
`anthropics/claude-code-action@v1` com `prompt: "/revisao-semanal"`, `schedule`
semanal, `actions/checkout` com `fetch-depth: 0` (sem histórico completo não há
semana para ler) e o secret `ANTHROPIC_API_KEY`. Em modo automação a saída vai
para o log da execução, então a skill precisaria publicar o resultado ela mesma.
Sendo o repositório público, o GitHub desativa o cron após 60 dias sem atividade.
