#!/bin/bash

set -e

mkdir -p .git/hooks

# Não há hook de pre-commit.
#
# Havia um que rodava `make format`, e ele foi removido: o `make format` roda
# black, ruff --fix e sqlfmt no repositório inteiro, e não nos arquivos que estão
# sendo commitados. Na prática ele reformatava arquivos que a pessoa não tocou e
# abortava o commit por erros de lint pré-existentes em código de terceiros —
# barrando commit que não tinha relação nenhuma com o problema.
#
# A formatação continua disponível à mão (`make format`), a verificação continua
# no pre-push (`make lint`) e na CI.
rm -f .git/hooks/pre-commit

cat > .git/hooks/pre-push << 'EOF'
#!/bin/bash
set -e
echo "Running pre-push checks..."
make lint -e GITLAB_CI=TRUE
make test
echo -e "\033[0;32mPre-push checks passed!\033[0m"
exit 0
EOF

chmod +x .git/hooks/pre-push

echo "Git hooks setup complete!"
echo "Installed hooks:"
echo "  - pre-push: roda 'make lint' e 'make test' antes de enviar"
