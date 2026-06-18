FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir uv

ENV PYTHONUNBUFFERED=1

EXPOSE 8000

CMD ["uvx", "astro-airflow-mcp", "--transport", "http", "--host", "0.0.0.0", "--port", "8000"]