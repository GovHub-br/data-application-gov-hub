## Descrição
<!-- O que esse PR faz? Por que essa mudança é necessária? -->

## Tipo de mudança
- [ ] Nova funcionalidade / pipeline
- [ ] Correção de bug ou inconsistência de dados
- [ ] Refatoração de modelo DBT
- [ ] Documentação
- [ ] Infraestrutura / CI
- [ ] Outro: ___

## Issues relacionadas
Closes #

## Como testar / validar

```bash
# Exemplo — ajuste conforme o tipo de mudança

# Para modelos DBT
dbt test --select <nome_do_modelo>

# Para DAGs
airflow dags test <nome_da_dag> <data_execucao>

# Para testes gerais
make test
```

## Evidências
<!-- Prints, logs, resultados de query ou link de documentação -->

## Checklist
- [ ] Testes DBT adicionados/atualizados
- [ ] Documentação atualizada
- [ ] Sem dados sensíveis ou credenciais no código
- [ ] Branch atualizada com `upstream/main`
