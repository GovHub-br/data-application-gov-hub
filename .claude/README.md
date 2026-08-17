# `.claude/`

Configuração do Claude Code **versionada**, para que a equipe receba tudo ao
clonar — sem instalar nada, sem registrar marketplace, sem rodar comando.

| Arquivo | O que é | Versionado? |
|---|---|---|
| `settings.json` | permissões que valem para toda a equipe | **Sim** |
| `skills/` | as dez skills do repositório — ver [`skills/README.md`](skills/README.md) | **Sim** |
| `settings.local.json` | suas preferências pessoais | Não, está no `.gitignore` |

## O `settings.json`

Tem duas listas, e as duas existem por motivos diferentes.

**`allow`** — comandos que o Claude roda sem parar para pedir confirmação. São
todos de leitura (`git status`, `gh pr view`, `docker logs`) ou alvos do próprio
`Makefile`. O objetivo é reduzir interrupção no trabalho rotineiro; nada que
escreva em banco, envie para o GitHub ou apague arquivo está aqui.

**`deny`** — leitura bloqueada, e esta é a parte que **não deve ser removida sem
pensar**:

```json
"deny": [
  "Read(./.env)", "Read(./infra/.env)", "Read(./infra/env/.env)",
  "Read(./infra/docker/.env)", "Read(./**/*.p12)", "Read(./**/*.ovpn)"
]
```

São os arquivos de credencial e de VPN. O `.gitignore` já impede que eles entrem
no git, mas isso não impede que o conteúdo seja **lido para dentro de uma
conversa** — e uma senha que aparece numa transcrição não é revogada junto com o
arquivo. O `deny` fecha esse caminho.

Este repositório lida com CPF, CNPJ e dados de raça e deficiência de agentes
culturais. Vale tratar o `deny` como parte da postura de segurança, não como
conveniência.

## Editar

`settings.json` é da equipe: mudança nele passa por pull request, como qualquer
outra. Preferência sua vai em `settings.local.json`, que é ignorado pelo git.
