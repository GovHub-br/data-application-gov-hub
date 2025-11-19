# Modelo de Mensagem de Commit (Conventional Commits)

Este documento serve como um guia rápido para a criação de mensagens de commit padronizadas. O uso deste formato é essencial para manter o histórico do projeto legível, facilitar a automação e gerar changelogs de forma automática.

## Estrutura Principal

Cada mensagem de commit consiste em um cabeçalho, um corpo opcional e um rodapé opcional. A estrutura é a seguinte:

```md
<tipo>[escopo opcional]: <descrição>

[corpo opcional]

[rodapé(s) opcional(is)]
```

-   **tipo**: Obrigatório. Define a categoria da mudança (ex: `feat`, `fix`, `docs`).
-   **escopo**: Opcional. Especifica a parte do código que foi alterada (ex: `api`, `parser`, `database`).
-   **descrição**: Obrigatório. Um resumo conciso da mudança, em letras minúsculas e sem ponto final.
-   **corpo**: Opcional. Fornece mais contexto, explicando o "porquê" da mudança. Separado da descrição por uma linha em branco.
-   **rodapé**: Opcional. Usado para referenciar issues (ex: `Refs: #42`) ou para declarar _breaking changes_ (ex: `BREAKING CHANGE:...`).

## Tipos de Commit Recomendados

| Tipo       | Descrição                                                                      |
| :--------- | :----------------------------------------------------------------------------- |
| `feat`     | Introduz uma nova funcionalidade ou capacidade.                                |
| `fix`      | Corrige um bug ou erro no código.                                              |
| `docs`     | Alterações relacionadas exclusivamente à documentação.                         |
| `refactor` | Alterações no código que não corrigem um bug nem adicionam uma funcionalidade. |
| `perf`     | Uma mudança de código que melhora o desempenho.                                |
| `test`     | Adição ou correção de testes automatizados.                                    |
| `build`    | Mudanças que afetam o sistema de build ou dependências externas.               |
| `ci`       | Mudanças nos arquivos e scripts de configuração de Integração Contínua (CI).   |
| `chore`    | Outras mudanças que não modificam o código-fonte ou os testes.                 |
| `style`    | Mudanças de estilo de código que não afetam a lógica (formatação, etc.).       |

### Exemplos Práticos

**1. Commit de correção de bug (fix):**

`fix: corrige cálculo de offset na paginação da API`

**2. Commit de nova funcionalidade (feat) com escopo:**

```md
feat(parser): adiciona suporte para o formato de dados do TSE

Refs: #45
```

**3. Commit com corpo para mais detalhes:**

```md
perf(database): otimiza query para busca de metadados

A query anterior utilizava um JOIN desnecessário que causava lentidão
em datasets com mais de 10.000 registros. Esta mudança simplifica
a consulta e adiciona um índice na coluna de metadados.
```

**4. Commit que fecha uma issue do GitHub:**

```md
fix(ui): resolve problema de renderização de tabelas no Firefox

Closes: #78
```
