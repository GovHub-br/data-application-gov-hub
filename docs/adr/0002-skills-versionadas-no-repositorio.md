# ADR 0002 — Skills versionadas no repositório, não instaladas por marketplace

- **Data:** 2026-08-13
- **Status:** aceito

## Contexto

O [GovHub-skills](https://github.com/GovHub-br/GovHub-skills) é um marketplace de
plugins do Claude Code com 51 skills, e traz máquina de manutenção real:
`scripts/atualizar-manifestos.py`, `scripts/validar-plugins.sh`, `NOTICE.md` para
procedência, plugins por categoria.

Oito das dez skills previstas para este repositório já existem lá. Duas não
existem, e duas das oito precisam de adaptação profunda — a
`govhub-pipeline-guide` aponta para o `data-application-gov-hub` e para os
projetos dbt `mir` e `ipea`, que não são os daqui.

O `.gitignore` deste repositório ignorava `.claude/` inteiro, então nenhuma skill
commitada chegava a quem clonava.

## Decisão

Versionar **todas as dez** em `.claude/skills/`. As específicas do MinC escritas
aqui; as genéricas copiadas do GovHub-skills, cada uma com um bloco
`PROCEDÊNCIA` registrando repositório de origem, pasta e commit.

Nenhum marketplace é registrado, nenhum plugin é instalado.

## Por quê

O critério que decide onde uma skill mora: **se o conhecimento dela sobrevive a
um `git mv` neste repositório, ele pertence ao GovHub-skills; se não sobrevive,
mora aqui.** Uma skill que cita `dbt/minc` e `plugins/cliente_*.py` tem a mesma
vida útil que o repositório — quando alguém renomeia uma pasta, o PR que renomeia
é o mesmo que conserta a skill, e o revisor vê as duas coisas juntas.
Centralizada, ela apodrece em silêncio até errar.

Esse critério apontaria para o modelo híbrido — genéricas no marketplace,
específicas aqui. A equipe escolheu versionar tudo, por atrito de uso: skill no
repositório carrega ao clonar, sem passo nenhum; plugin declarado em
`.claude/settings.json` avisa e **espera a pessoa aceitar a instalação** antes de
carregar. Um passo a mais é um passo em que alguém trava.

O repositório irmão já provou o padrão local: o `data-application-cidades` mantém
a `atualizar-docs-pages` em `.claude/skills/`, deeply acoplada ao `dominios.yml`
e ao `tooling/build.py` de lá. E provou também o custo de errar o critério — o
`references/layout.md` dela repete a paleta da marca que a `govhub-visual-identity`
já guarda no GovHub-skills.

## Alternativas descartadas

**Só marketplace.** Atualização central e uma fonte da verdade. Descartada porque
exige que cada pessoa aceite a instalação, e porque as quatro skills específicas
do MinC não têm por que existir fora deste repositório.

**Híbrido — genéricas por marketplace, específicas aqui.** Era a recomendação
técnica. Descartada pela equipe: dois lugares para editar skill é uma pergunta a
mais que alguém vai errar, e o ganho não compensou o atrito de instalação.

## Consequências

Correção feita no GovHub-skills **não chega sozinha aqui**. Atualizar é recopiar
a pasta, trocar o commit no bloco de procedência e reaplicar as adaptações locais
— a `commit-smart` tem uma seção "Neste repositório" que a cópia crua apaga. O
procedimento está em `.claude/skills/README.md`.

A `accountability-report` vinha com `scripts/node_modules/` empacotado no
original. Foi copiada sem essa pasta, mantendo o `package.json`, para não trazer
`node_modules` para dentro do git.

O caminho de promoção continua aberto e num sentido só: repo → prova de uso →
generaliza → GovHub-skills. A `abrir-pr` e a `to-issues-minc` são candidatas, se
os outros `data-application-*` adotarem os mesmos formulários.
