# docs-pages

Site de documentação dos dados do MinC, publicado no GitHub Pages.

Existe porque o `dbt docs` não serve ao público deste projeto. Ele é um catálogo
técnico — fala de materialização, contrato e SQL compilado — e quem precisa
destes dados também é de geografia, estatística, comunicação e gestão. Este site
responde antes o que cada conjunto de dados significa e o que é preciso saber
para citá-lo sem errar.

## Como rodar

```bash
make docs-serve
```

O primeiro `make docs-*` cria um virtualenv em `docs-pages/.venv` com `jinja2` e
`pyyaml`. **Não é preciso ter Airflow, dbt, Poetry nem acesso ao banco.**

| Comando | O que faz | Precisa de rede |
|---|---|---|
| `make docs-collect` | Atualiza o acervo em `src/_data/` lendo o repositório | sim (git, gh) |
| `make docs-build` | Renderiza o site em `docs-pages/site/` | **não** |
| `make docs-serve` | Build e servidor em `localhost:8000` | não |
| `make docs-clean` | Apaga o site e o virtualenv | não |

`docs-collect` e `docs-build` são separados de propósito: o build no CI nunca
depende de rede nem de banco, e o diff de uma coleta mostra exatamente o que
mudou no repositório desde a última.

## Estrutura

```
docs-pages/
├── tooling/
│   ├── collect.py       orquestra os coletores
│   ├── collectors/      dbt, DAGs e entregas — leem o repo, nunca o banco
│   ├── dados.py         cruza acervo com curadoria e calcula as métricas
│   ├── linhagem.py      gera o SVG do caminho de cada tabela gold
│   └── build.py         renderiza os templates
├── src/
│   ├── _data/           acervo coletado (JSON, versionado)
│   ├── dominios.yml     ← O ÚNICO ARQUIVO ESCRITO À MÃO
│   ├── templates/       Jinja2, uma página por template
│   └── assets/tema.css
└── site/                saída do build (ignorada pelo git)
```

## De onde vem cada dado

| Coletor | Lê | Como |
|---|---|---|
| `dbt_models.py` | `dbt/minc/models/**/*.sql` e `schema.yml` | regex nos `ref()`/`source()`, YAML nas descrições — **sem dbt instalado** |
| `airflow_dags.py` | `dags/data_ingest/**` e `plugins/cliente_*.py` | `ast`, parse estático — **sem importar Airflow** |
| `entregas.py` | histórico de commits e PRs | `git` e `gh` |

Nada vem do banco. Nada é digitado num template.

## O que você edita

**`src/dominios.yml`.** É a curadoria: o que cada conjunto de dados significa,
que perguntas responde e o que é preciso saber antes de citar seus números.
Tudo o mais é lido do repositório.

Se um **número** está errado no site, a correção nunca é no texto — é no
coletor ou no código que ele lê. Se uma **explicação** está errada, é no
`dominios.yml`.

## Onde mexer, por sintoma

| O que você quer | Onde |
|---|---|
| Números do site estão velhos | `make docs-collect` — não edite texto |
| Reescrever o que um conjunto significa | `dominios.yml` → `contexto`, `o_que_faz` |
| Mudar as perguntas que um conjunto responde | `dominios.yml` → `perguntas` |
| Acrescentar uma ressalva sobre um número | `dominios.yml` → `armadilhas` |
| Incluir um conjunto novo | novo item em `dominios.yml` → `dominios` |
| Texto de abertura de uma página | o `.j2` correspondente em `templates/` |
| Aparência, espaçamento, cor | `assets/tema.css` → tokens `--esp-*` e as cores do topo |
| Aparência dos diagramas de linhagem | `tooling/linhagem.py` e as regras `.lin-*` do CSS |
| Uma métrica nova na página | `tooling/dados.py` → `metricas` |

## Quando o build falha

| Mensagem | Causa |
|---|---|
| `acervo vazio` | falta rodar `make docs-collect` |
| `falta a curadoria` | `src/dominios.yml` ausente ou movido |
| `domínio 'x' não casou nenhum modelo` | o `slug` não bate com a pasta em `models/`. Quase sempre erro de digitação — ou declare `sem_modelos: true` |
| `link quebrado` | um `href` aponta para página que não existe |
| `'x' is undefined` | o template usa variável que `dados.py` não fornece |
| `domínios sem curadoria` | aviso, não erro: existe pasta em `models/` sem entrada no `dominios.yml`, e ela fica fora do site |

## Regras

1. **Nunca digite um número num template.** Se o número que você quer não
   existe, ele se calcula em `dados.py` e o template consome. Número escrito à
   mão mente na primeira mudança do repositório, e ninguém percebe.
2. **Nunca edite `src/_data/*.json` à mão.** A próxima coleta sobrescreve, e o
   número passa a mentir em silêncio.
3. **Commite o acervo junto.** Os JSONs são versionados: sem eles, o CI não
   reproduz o site.
4. **Rode `make docs-serve` antes de commitar.** O build falha de propósito em
   link interno quebrado — erro que passaria batido na revisão.
5. **Toda ressalva conhecida vira `armadilhas`.** Um número sem o seu limite ao
   lado é um número que vai ser citado errado.
6. **Escreva o problema antes da solução.** "Ninguém sabia se a cota estava
   sendo cumprida" explica mais que "modelagem dimensional das cotas".

## Antes de considerar pronto

- [ ] `make docs-serve` roda sem erro e você abriu as páginas alteradas
- [ ] Nenhum número foi digitado à mão
- [ ] O acervo está no commit, se você rodou a coleta
- [ ] O texto novo diz o que o conjunto resolve, não o que o código faz
- [ ] Todo conjunto com número sensível tem a sua `armadilhas` preenchida
