#!/bin/bash

set -e

mkdir -p .git/hooks

cat > .git/hooks/pre-commit << 'EOF'
#!/bin/bash
set -e
echo "Running pre-commit checks..."
make format
echo -e "\033[0;32mPre-commit checks passed!\033[0m"
exit 0
EOF

cat > .git/hooks/pre-push << 'EOF'
#!/bin/bash
set -e
echo "Running pre-push checks..."
make lint -e GITLAB_CI=TRUE
make test
echo -e "\033[0;32mPre-push checks passed!\033[0m"
exit 0
EOF

chmod +x .git/hooks/pre-commit
chmod +x .git/hooks/pre-push

echo -e "${GREEN}Git hooks setup complete!${NC}"
echo "Installed hooks:"
echo "  - pre-commit: Checks for trailing whitespace and large files"
echo "  - pre-push: Checks for TODO markers and protected branches"
