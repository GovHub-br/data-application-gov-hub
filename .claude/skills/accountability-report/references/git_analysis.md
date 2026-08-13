# Comandos de análise do git

Comandos prontos para extrair os números do relatório. Ajuste `SINCE`/`UNTIL` ao
período acordado (datas absolutas, formato `AAAA-MM-DD`). Rode-os a partir da raiz do
repositório. Todos foram validados em uso real.

```bash
SINCE="2025-06-22"; UNTIL="2026-06-22"
```

## Totais e janela real do repositório

```bash
# Total de commits no período
git log --since="$SINCE" --until="$UNTIL" --oneline | wc -l

# Primeiro e último commit do repo (para detectar se o período > vida do projeto)
git log --reverse --format="%ad %h %s" --date=short | head -1
git log --format="%ad %h %s" --date=short | head -1
```

Se o primeiro commit do repo for mais recente que `SINCE`, o período efetivo é "desde o
início". Registre isso na nota metodológica do relatório.

## Commits por mês (para a tabela de distribuição temporal)

```bash
git log --since="$SINCE" --until="$UNTIL" --format="%ad" --date=format:"%Y-%m" \
  | sort | uniq -c
```

Para descobrir o "marco principal" de cada mês, liste as features daquele mês:

```bash
git log --since="$SINCE" --until="$UNTIL" --format="%ad|%s" --date=format:"%Y-%m" \
  | grep -iE "feat\(|feat:|cria|new" | sort | awk -F'|' '{print $1" :: "$2}'
```

## Autores (tabela de equipe)

```bash
git log --since="$SINCE" --until="$UNTIL" --format="%an" | sort | uniq -c | sort -rn
```

Nomes diferentes podem ser a mesma pessoa (configs de git distintas). Sinalize no
relatório, mas **não** consolide silenciosamente — a transparência é o ponto.

## Tipos de commit (feat vs fix etc.) — convencional

```bash
git log --since="$SINCE" --until="$UNTIL" --format="%s" \
  | grep -oE "^[a-z]+(\([^)]+\))?" | sed -E 's/\(.*\)//' | sort | uniq -c | sort -rn
```

## Escopos mais comuns (revelam os TEMAS de negócio)

```bash
git log --since="$SINCE" --until="$UNTIL" --format="%s" \
  | grep -oE "\([^)]+\)" | sort | uniq -c | sort -rn | head -40
```

## Pull Requests integrados

```bash
# Quantidade de PRs mesclados
git log --since="$SINCE" --until="$UNTIL" --merges --oneline | grep -c "pull request"

# Faixa de números de PR (menor e maior)
git log --merges --format="%s" | grep -oE "#[0-9]+" | tr -d '#' | sort -n | sed -n '1p;$p'

# Nomes das branches mescladas (ótima fonte de TEMAS — agrupe por prefixo:
# feat/, fix/, explorer/, dash/, feature/...)
git log --since="$SINCE" --until="$UNTIL" --merges --format="%s" \
  | sed -E 's/Merge pull request #[0-9]+ from [^/]+\///' | sort | uniq -c | sort -rn
```

## Dica de interpretação

Os **escopos** dos commits e os **prefixos das branches** são o melhor atalho para
descobrir como o trabalho se organiza em temas de negócio. Um escopo recorrente como
`(odonto)`, `(consistency)` ou `(auth)` quase sempre corresponde a uma seção natural
do relatório. Cruze isso com o inventário do código atual para confirmar que o tema
ainda existe (não foi removido) e para achar o nome amigável de cada entrega.
