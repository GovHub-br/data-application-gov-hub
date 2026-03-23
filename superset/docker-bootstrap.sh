#!/bin/bash
set -e

ADMIN_USERNAME="${SUPERSET_ADMIN_USERNAME:-admin}"
ADMIN_FIRSTNAME="${SUPERSET_ADMIN_FIRSTNAME:-Admin}"
ADMIN_LASTNAME="${SUPERSET_ADMIN_LASTNAME:-User}"
ADMIN_EMAIL="${SUPERSET_ADMIN_EMAIL:-admin@superset.com}"
ADMIN_PASSWORD="${SUPERSET_ADMIN_PASSWORD:-admin}"

echo "==> Running database migrations..."
superset db upgrade

echo "==> Creating Superset admin user..."
superset fab create-admin \
  --username "$ADMIN_USERNAME" \
  --firstname "$ADMIN_FIRSTNAME" \
  --lastname "$ADMIN_LASTNAME" \
  --email "$ADMIN_EMAIL" \
  --password "$ADMIN_PASSWORD" 2>/dev/null || true

echo "==> Initializing Superset roles and permissions..."
superset init

# Import any dashboard exports that were committed to the repository
EXPORTS_DIR="/app/superset_home/exports"
if [ -d "$EXPORTS_DIR" ]; then
  FOUND=0
  for file in "$EXPORTS_DIR"/*.json "$EXPORTS_DIR"/*.zip; do
    [ -f "$file" ] || continue
    FOUND=1
    echo "==> Importing Superset assets from: $file"
    if ! superset import-dashboards -p "$file" 2>&1; then
      echo "    Warning: could not import $file – see output above for details."
    fi
  done
  [ "$FOUND" -eq 0 ] && echo "==> No exported dashboards found in $EXPORTS_DIR – skipping import."
fi

echo "==> Starting Superset on port 8088..."
exec superset run -p 8088 -h 0.0.0.0 --with-threads --reload --debugger
