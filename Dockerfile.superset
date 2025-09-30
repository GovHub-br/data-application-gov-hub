FROM apache/superset:latest

USER root

# Install PostgreSQL driver directly in the virtual environment site-packages
# This ensures the driver is available when Superset tries to connect to PostgreSQL
RUN /usr/local/bin/python -m pip install psycopg2-binary --target /app/.venv/lib/python3.10/site-packages/

# Install other useful database drivers for future use
RUN /usr/local/bin/python -m pip install pymysql --target /app/.venv/lib/python3.10/site-packages/

USER superset