## Descrição
<!-- O que esse PR faz? Por que essa mudança é necessária? -->

> Antes de abrir este PR, consulte o [Protocolo de Aprovação de Pull Requests](https://github.com/GovHub-br/data-application-gov-hub/blob/main/.github/MERGE_REQUEST_PROTOCOL.md).

## Tipo de mudança
- [ ] Nova funcionalidade / pipeline
- [ ] Correção de bug ou inconsistência de dados
- [ ] Refatoração de modelo DBT
- [ ] Documentação
- [ ] Infraestrutura / CI
- [ ] Outro: ___

## Issues relacionadas
Closes #

## Domínio de revisão
<!-- Marque o domínio/time principal impactado por este PR. Use labels team:* quando ajudar na triagem. -->

- [ ] GCES / OSS
- [ ] IPEA
- [ ] MIR
- [ ] MCid
- [ ] MinC
- [ ] OSS
- [ ] Múltiplos domínios: ___

## Como testar / validar

```bash
# Exemplo: ajuste conforme o tipo de mudança

# Para modelos DBT
cd airflow_lappis/dags/dbt/<projeto>
dbt test --select <nome_do_modelo>

# Para DAGs
airflow dags test <nome_da_dag> <data_execucao>

# Para testes gerais
make lint
make test
```

## Evidências
<!-- Prints, logs, resultados de query ou link de documentação -->

## Checklist
- [ ] Título do PR segue Conventional Commits
- [ ] Issue relacionada foi referenciada
- [ ] Testes/lint foram executados ou a ausência foi justificada
- [ ] Testes DBT adicionados/atualizados, se aplicável
- [ ] Documentação atualizada, se aplicável
- [ ] Sem dados sensíveis ou credenciais no código
- [ ] Branch atualizada com `upstream/main` ou `origin/main`
- [ ] Revisores automáticos por domínio foram solicitados pelo GitHub ou justificados no PR
