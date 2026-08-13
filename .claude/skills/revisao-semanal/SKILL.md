---
name: revisao-semanal
description: >-
  Gera a revisão semanal do repositório a partir dos commits da semana na main e
  nas outras branches, dos pull requests, e das issues fechadas e abertas —
  dizendo o que foi feito e o que não andou. Use sempre que o usuário pedir
  "revisão da semana", "relatório semanal", "o que foi feito essa semana", "como
  está o andamento", "resumo da semana", "o que travou", ou quiser preparar o
  acompanhamento das metas para a coordenação. Também quando pedir o mesmo para
  outro período ("últimas duas semanas", "desde o dia X").
allowed-tools: Bash, Read, Grep, Glob
---

# Revisão semanal

Produz um relatório honesto do que andou e do que não andou no repositório
durante um período, para servir de insumo ao acompanhamento das metas.

## O princípio que decide se isso presta

**Os números vêm do script. A narrativa vem de você.**

Um relatório de acompanhamento que superestima é pior que relatório nenhum,
porque a coordenação decide em cima dele. E o jeito de errar é sempre o mesmo:
contar de cabeça. Por isso a apuração é um script, e ele roda antes de qualquer
leitura sua.

Você nunca escreve um número que não esteja na saída do script. Se o número que
você quer não existe lá, ou você o obtém com um comando adicional, ou você diz
no relatório que não foi possível apurar. **Estimar está proibido.**

O que só você faz: ler o diff e o corpo da issue para dizer *o que* foi feito,
cruzar as fontes para perceber o que não fecha, e nomear o que está parado.

## Passo 1 — Apurar

```bash
./.claude/skills/revisao-semanal/scripts/apurar.sh 7
```

O argumento é o número de dias; o padrão é 7. Se o usuário pediu outro período,
passe o número correspondente e diga no relatório qual período foi coberto.

O script coleta: commits na `main`, commits nas outras branches que ainda não
entraram na `main`, PRs mergeados e abertos, issues fechadas e abertas, e o
vocabulário de labels. Leia a saída inteira antes de escrever qualquer coisa.

## Passo 2 — Ler a substância

A apuração diz *quanto*. Agora descubra *o quê*:

- Para os commits relevantes, leia o diff: `git show --stat <hash>` e, quando o
  título não bastar, o diff em si. O que interessa é a mudança de comportamento,
  não a contagem de linhas.
- Para as issues fechadas, leia o corpo e os comentários: `gh issue view <n> --comments`.
- Para os PRs mergeados, veja o que eles fecharam: `gh pr view <n>`.

Traduza para o vocabulário do domínio. "Refatorou `extracao_bbagil_dag.py`" não
diz nada a quem acompanha meta; "a extração do BB Ágil deixou de depender de
arquivo local e passa a ler do Postgres" diz.

## Passo 3 — Cruzar

É aqui que aparece o que ninguém percebeu. Procure especificamente por:

| Sinal | O que costuma significar |
|---|---|
| Branch com commits da semana, sem PR aberto | Trabalho pronto que ninguém submeteu |
| Branch com commits antigos e nenhuma atividade | Trabalho abandonado, ou esquecido |
| PR aberto sem review há mais de uma semana | Fila travada — o gargalo não é quem escreve |
| Issue aberta há meses sem comentário nem commit | Ou virou irrelevante, ou está bloqueada em silêncio |
| Muitos commits na main sem issue fechada | Trabalho acontecendo fora do rastreamento |
| Issue fechada sem commit que a referencie | Fechada por desistência, ou o vínculo se perdeu |

Cada um desses é uma frase do relatório, com o número da issue ou o nome da
branch — nunca uma observação genérica.

## Passo 4 — Agrupar por meta

Se existirem labels `meta-*`, agrupe por elas e mantenha um balde para as sem
meta. O balde é informativo: se ele cresce, o tempo está indo para fora do alvo.

**Se as labels `meta-*` não existirem** — confira na seção de vocabulário da
apuração — não agrupe por palpite a partir do título. Escreva o relatório sem
agrupamento e registre, no fim, que o agrupamento por meta depende de criar essas
labels. Adivinhar a meta a partir do título é exatamente o tipo de número
inventado que este relatório não pode ter.

## Passo 5 — Escrever

Estrutura fixa, nesta ordem:

```markdown
# Revisão da semana — <data inicial> a <data final>

## Em uma frase
<O que a semana foi. Uma linha, sem enrolação.>

## O que foi feito
<Por meta, se houver label. Cada item diz o que mudou no comportamento do
sistema, com o número do PR ou da issue entre parênteses.>

## O que não andou
<Issues abertas sem movimento no período, com há quanto tempo. PRs parados
esperando review, com há quantos dias. Branches com trabalho não submetido.>

## O que merece atenção
<Só o que exige decisão de alguém. Se não houver nada, escreva "nada" — não
encha esta seção para ela parecer útil.>

## Números
<Tabela com o que o script apurou: commits, PRs mergeados, issues fechadas,
issues abertas. Nada aqui pode ter saído da sua cabeça.>

---
*Apurado em <data> sobre <período>. Os números vêm de `git` e da API do GitHub;
o texto é interpretação.*
```

## O limite do que dá para afirmar

Sobre "o que não foi feito", você só pode dizer o que é observável:

> ✅ "A #10 está aberta desde 08/06 e não teve commit nem comentário no período."
>
> ❌ "A #10 está atrasada." — atrasada em relação a quê? Você não sabe o que foi
> combinado.

A skill não conhece o plano da equipe, só o rastro que ele deixou. Descrever o
rastro é útil; inferir intenção a partir dele é inventar. Quando a diferença
importar, escreva a versão observável e deixe o julgamento para quem lê.

## Onde publicar

Por padrão, entregue o relatório na conversa. Se o usuário pedir para publicar,
comente numa issue de acompanhamento em vez de criar arquivo commitado — o
histórico do repositório não deve encher de relatório semanal:

```bash
gh issue comment <numero-da-issue-de-acompanhamento> --body-file <arquivo>
```

## Antes de considerar pronto

- [ ] Todo número do relatório aparece na saída do `apurar.sh`
- [ ] Cada item de "o que foi feito" cita PR ou issue
- [ ] Cada item de "o que não andou" diz há quanto tempo
- [ ] Nenhuma frase afirma intenção que só o rastro não sustenta
- [ ] Se faltam as labels `meta-*`, isso está registrado no fim
- [ ] A seção "merece atenção" tem só o que exige decisão — ou diz "nada"
