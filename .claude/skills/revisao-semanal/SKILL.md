---
name: revisao-semanal
description: >-
  Gera a revisão semanal do repositório a partir do que foi entregue no período —
  commits que entraram na main, pull requests mergeados, issues fechadas e o
  trabalho em andamento nas branches que receberam commit — com saída em
  Markdown, HTML e PDF A4. Use sempre que o usuário pedir "revisão da semana",
  "relatório semanal", "o que foi feito essa semana", "o que entregamos",
  "resumo da semana", "pdf da revisão", ou quiser preparar o acompanhamento das
  metas para a coordenação. Também quando pedir o mesmo para outro período
  ("últimas duas semanas", "desde o dia X").
allowed-tools: Bash, Read, Grep, Glob
---

# Revisão semanal

Produz um relatório honesto do que foi **entregue** no repositório durante um
período, para servir de insumo ao acompanhamento das metas.

## O que este relatório não é

Não é um inventário do que está parado.

Aqui o trabalho vai da extração do dado até a visualização, e esse ciclo não cabe
numa semana. Uma branch sem commit nos últimos sete dias é o ritmo normal do
processo, não trabalho abandonado — e listá-la toda semana como "não andou"
produzia alarme falso e enterrava a entrega no meio do ruído.

O recorte é: **o que entrou, e o que se mexeu no caminho para entrar.** Branch
que não recebeu commit no período não aparece, nem para dizer que não recebeu.
O `apurar.sh` já as filtra, para a regra não depender de disciplina de quem
escreve.

## O princípio que decide se isso presta

**Os números vêm do script. A narrativa vem de você.**

Um relatório de acompanhamento que superestima é pior que relatório nenhum,
porque a coordenação decide em cima dele. E o jeito de errar é sempre o mesmo:
contar de cabeça. Por isso a apuração é um script, e ele roda antes de qualquer
leitura sua.

Você nunca escreve um número que não esteja na saída do script. Se o número que
você quer não existe lá, ou você o obtém com um comando adicional, ou você diz
no relatório que não foi possível apurar. **Estimar está proibido.**

O que só você faz: ler o diff para dizer *o que* foi entregue, e traduzir isso
para o vocabulário de quem acompanha meta.

## Passo 1 — Apurar

```bash
./.claude/skills/revisao-semanal/scripts/apurar.sh 7
```

O argumento é o número de dias; o padrão é 7. Se o usuário pediu outro período,
passe o número correspondente e diga no relatório qual período foi coberto.

O script coleta: commits na `main`, commits das branches que **receberam commit
no período** e ainda não entraram na `main`, PRs mergeados e abertos, issues
fechadas no período, issues abertas **com movimento na janela** e o vocabulário
de labels. Leia a saída inteira antes de escrever qualquer coisa.

Dois filtros já vêm aplicados, e nenhum dos dois se contorna à mão:

- **Branch sem commit no período não aparece.**
- **Issue aberta sem movimento no dobro do período não aparece** — 14 dias no
  padrão de 7. Backlog parado há meses é outro assunto; num relatório do que foi
  entregue ele é ruído, e o relatório fica pior por citá-lo.

PRs e issues abertos saem na apuração, mas não viram seção do relatório: eles só
entram quando a entrega do período mexe com eles — ver o Passo 3.

**Se alguma seção sair como `[FALHA NA API...]`**, a API do GitHub não respondeu
— o script já tentou três vezes antes de desistir. Aquela seção **não tem dado**:
não escreva número nem "nenhum" para ela. Rode de novo; se persistir, registre no
relatório que aquele recorte não pôde ser apurado. Um zero que veio de falha de
rede é exatamente o número inventado que esta skill existe para impedir, e é o
mais perigoso deles, porque parece uma semana tranquila.

## Passo 2 — Ler a substância

A apuração diz *quanto*. Agora descubra *o quê*:

- Para os commits que entraram, leia o diff: `git show --stat <hash>` e, quando o
  título não bastar, o diff em si. O que interessa é a mudança de comportamento,
  não a contagem de linhas.
- Para os PRs mergeados, veja o que eles fecharam: `gh pr view <n>`.
- Para as issues fechadas, leia o corpo e os comentários: `gh issue view <n> --comments`.
- Para as branches com commit no período, leia os commits: o relatório precisa
  dizer **sobre o que era aquele trabalho**, não só que houve trabalho.

Traduza para o vocabulário do domínio. "Refatorou `extracao_bbagil_dag.py`" não
diz nada a quem acompanha meta; "a extração do BB Ágil deixou de depender de
arquivo local e passa a ler do Postgres" diz.

## Passo 3 — Cruzar

É aqui que aparece o que ninguém percebeu. Procure especificamente por:

| Sinal | O que costuma significar |
|---|---|
| Branch com commits do período, sem PR aberto | Trabalho pronto que ninguém submeteu |
| Entrega que contradiz uma issue aberta | A issue ficou desatualizada pela própria entrega |
| Entrega que altera arquivo disputado por outra branch ativa | Conflito a caminho, ainda invisível |
| PR mergeado que exige configuração para valer | A entrega existe no código, mas não no ambiente |
| Muitos commits na main sem issue fechada | Trabalho acontecendo fora do rastreamento |

Cada um desses é uma frase do relatório, com o número da issue ou o nome da
branch — nunca uma observação genérica.

**O que não é sinal:** branch parada, issue fora da janela, PR aberto há tempo.
Isso é o ciclo do projeto, não novidade do período. Não vá procurar — e não vá
buscar por fora o que os filtros da apuração tiraram.

## Passo 4 — Agrupar por meta

Se existirem labels `meta-*`, agrupe as entregas por elas e mantenha um balde
para as sem meta. O balde é informativo: se ele cresce, o tempo está indo para
fora do alvo.

**Se as labels `meta-*` não existirem** — confira na seção de vocabulário da
apuração — não agrupe por palpite a partir do título. Escreva o relatório sem
agrupamento e registre, no fim, que o agrupamento por meta depende de criar essas
labels. Adivinhar a meta a partir do título é exatamente o tipo de número
inventado que este relatório não pode ter.

## Passo 5 — Escrever

O relatório é **saída, não artefato do projeto**. Escreva num diretório
temporário e entregue de lá — nada de relatório dentro do repositório, nem
versionado nem ignorado:

```bash
DIR="$(mktemp -d)"
SAIDA="$DIR/revisao-semanal-<inicio>_a_<fim>.md"
```

**Crase com parcimônia.** Quem lê isto acompanha meta, não escreve código. Use
crase só para o que é literalmente um identificador que a pessoa vai procurar —
nome de arquivo, de tabela, de branch, de variável. Não marque nome de
ferramenta, de tecnologia nem termo do domínio: "a DAG do SALIC roda em SQL
Server" não leva crase em nada. Cada marcação a mais pica o parágrafo e o texto
passa a parecer log em vez de relatório.

Estrutura fixa, nesta ordem:

```markdown
# Revisão da semana — <data inicial> a <data final>

## Em uma frase
<O que o período entregou. Uma linha, sem enrolação.>

## O que foi entregue
<O que entrou na main no período. Por meta, se houver label. Cada item diz o que
mudou no comportamento do sistema, com o número do PR ou da issue.>

## Em andamento
<Só as branches que receberam commit no período e ainda não entraram na main: o
que aqueles commits fazem, e se já existe PR. Branch sem commit no período não
aparece aqui. Se nenhuma recebeu commit, escreva isso em uma linha.>

## O que merece atenção
<Só o que exige decisão de alguém, e só o que decorre da entrega do período. Se
não houver nada, escreva "nada" — não encha esta seção para ela parecer útil.>

## Números
<Tabela com o que o script apurou: commits na main, PRs mergeados, issues
fechadas, branches com commit no período. Nada aqui pode ter saído da sua cabeça.>

---
*Apurado em <data> sobre <período>. Os números vêm de `git` e da API do GitHub;
o texto é interpretação.*
```

## Passo 6 — Gerar o PDF

```bash
./.claude/skills/revisao-semanal/scripts/gerar_pdf.sh "$SAIDA"
```

Sai um `.html` e um `.pdf` A4 ao lado do `.md`, no mesmo diretório temporário,
com a faixa "Revisão Semanal · \<período\>" preenchida a partir do título do
relatório — por isso o `#` da primeira linha precisa seguir o formato
`# Revisão da semana — <início> a <fim>`.

**Identidade visual.** O documento sai na marca do GovHub: roxo `#7A34F3`,
tipografia Inter, faixa superior com a logo oficial. Os tokens são os de
`GovHub-skills/01-govhub/govhub-visual-identity`, e a logo está em
`assets/govhub-horizontal-light.svg`, embutida no HTML como data URI para o
arquivo abrir sozinho e o PDF não depender de rede.

O tema mora em `build_html.mjs`, aqui na skill, e **não** no conversor da
`accountability-report`. Motivo: aquele relatório é azul, de entrega oficial a
órgão, e é cópia versionada do GovHub-skills — editar lá se perde na próxima
recópia. Da `accountability-report` esta skill usa só o `html_to_pdf.sh`, que
não tem estilo nenhum e por isso pode ser compartilhado. Se aquela pasta sumir,
o script para com erro em vez de gerar um PDF fora do padrão.

Identificador no meio do texto sai **sem caixa, sem moldura e sem cor de
alerta** — só um peso maior. A marcação vermelha em caixa, que o tema azul usa,
lia como erro num documento de acompanhamento e picotava a leitura. Bloco de
código (crase tripla) continua com fundo e monoespaçada, que ali é o que ajuda.

Na primeira execução o script instala o `marked` dentro da pasta do conversor da
`accountability-report` — uma instalação só, que serve as duas skills. Demora
alguns segundos e não toca no `pyproject.toml` do projeto.

O PDF sai pelo **weasyprint**. Se ele faltar, `pipx install weasyprint`. O
`html_to_pdf.sh` prefere Chrome quando o acha no PATH, o que funciona no Linux;
no macOS o Chrome não é alcançável por symlink (aborta) e trava quando chamado
pelo caminho do bundle, então lá o caminho bom é o weasyprint mesmo.

Confira que o PDF existe antes de entregar — `file <arquivo>.pdf` deve dizer
"PDF document".

## O limite do que dá para afirmar

Você só pode dizer o que é observável:

> ✅ "O PR #20 alterou `primeiro_acesso_contemplados.sql`, arquivo que a branch
> da Meta 5 também toca."
>
> ❌ "A branch da Meta 5 vai dar conflito." — talvez quem cuida dela já tenha
> rebaseado. Você viu os arquivos, não o futuro.

A skill não conhece o plano da equipe, só o rastro que ele deixou. Descrever o
rastro é útil; inferir intenção a partir dele é inventar. Quando a diferença
importar, escreva a versão observável e deixe o julgamento para quem lê.

## Onde entregar

Entregue o texto na conversa e anexe o PDF. Os arquivos ficam no diretório
temporário e não são para guardar: o que os torna reproduzíveis é o `apurar.sh`
estar versionado, não o `.pdf` estar salvo em algum lugar.

Se o usuário pedir para publicar, comente numa issue de acompanhamento — nunca
commite o relatório:

```bash
gh issue comment <numero-da-issue-de-acompanhamento> --body-file "$SAIDA"
```

O PDF serve para quem recebe a revisão fora do GitHub — coordenação, reunião de
acompanhamento. Para anexar num comentário de issue é preciso subir pela
interface do GitHub; o `gh` não anexa arquivo.

## Antes de considerar pronto

- [ ] Todo número do relatório aparece na saída do `apurar.sh`
- [ ] Nenhuma seção que falhou na API virou "zero" ou "nenhum" no relatório
- [ ] Cada item de "o que foi entregue" cita PR ou issue
- [ ] "Em andamento" diz **sobre o que era** o trabalho, não só que houve trabalho
- [ ] Nenhuma branch sem commit no período foi citada, em nenhuma seção
- [ ] Nenhuma issue fora da janela foi citada, em nenhuma seção
- [ ] Crase só em identificador que a pessoa vai procurar — não em nome de
      tecnologia nem em termo do domínio
- [ ] Nenhuma frase afirma intenção que só o rastro não sustenta
- [ ] Se faltam as labels `meta-*`, isso está registrado no fim
- [ ] A seção "merece atenção" tem só o que decorre da entrega — ou diz "nada"
- [ ] Nenhum arquivo do relatório ficou dentro do repositório
- [ ] O PDF foi gerado e `file` confirma que é um PDF de verdade
