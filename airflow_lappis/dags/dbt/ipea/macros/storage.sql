{% macro configure_storage() %}
    {{ adapter.dispatch('configure_storage', 'ipea')() }}
{% endmacro %}

{% macro duckdb__configure_storage() %}
    {% set backend = env_var('OBJECT_STORAGE', env_var('STORAGE_BACKEND', 's3')) | lower %}

    {% if backend == 's3' %}
        {% set endpoint = env_var('S3_ENDPOINT', env_var('MINIO_ENDPOINT', '')) %}
        {% set region = env_var('AWS_REGION', env_var('MINIO_REGION', 'us-east-1')) %}

        create or replace secret storage (
            type s3,
            key_id '{{ env_var("AWS_ACCESS_KEY_ID", env_var("MINIO_ACCESS_KEY", "minioadmin")) }}',
            secret '{{ env_var("AWS_SECRET_ACCESS_KEY", env_var("MINIO_SECRET_KEY", "minioadmin")) }}',
            region '{{ region }}'
            {% if endpoint %}
                , endpoint '{{ endpoint | replace("http://", "") | replace("https://", "") }}'
                , use_ssl {{ env_var("S3_USE_SSL", env_var("MINIO_USE_SSL", "false")) }}
                , url_style '{{ env_var("S3_URL_STYLE", env_var("MINIO_URL_STYLE", "path")) }}'
            {% endif %}
        );
    {% elif backend == 'adls' %}
        create or replace secret storage (
            type azure,
            provider {{ env_var("AZURE_PROVIDER", "credential_chain") }},
            account_name '{{ env_var("AZURE_STORAGE_ACCOUNT") }}'
            {% if env_var("AZURE_TENANT_ID", "") %}
                , tenant_id '{{ env_var("AZURE_TENANT_ID") }}'
            {% endif %}
            {% if env_var("AZURE_CLIENT_ID", "") %}
                , client_id '{{ env_var("AZURE_CLIENT_ID") }}'
            {% endif %}
            {% if env_var("AZURE_CLIENT_SECRET", "") %}
                , client_secret '{{ env_var("AZURE_CLIENT_SECRET") }}'
            {% endif %}
        );
    {% elif backend == 'gcs' %}
        create or replace secret storage (
            type gcs,
            key_id '{{ env_var("GCS_HMAC_KEY") }}',
            secret '{{ env_var("GCS_HMAC_SECRET") }}'
        );
    {% else %}
        {{ exceptions.raise_compiler_error("Unsupported OBJECT_STORAGE '" ~ backend ~ "'. Expected one of: s3, adls, gcs.") }}
    {% endif %}
{% endmacro %}

{% macro default__configure_storage() %}
    select 1 as storage_skipped;
{% endmacro %}

{% macro storage_uri(layer, table) %}
    {{ return(adapter.dispatch('storage_uri', 'ipea')(layer, table)) }}
{% endmacro %}

{% macro default__storage_uri(layer, table) %}
    {% set backend = env_var('OBJECT_STORAGE', env_var('STORAGE_BACKEND', 's3')) | lower %}

    {% if backend == 's3' %}
        {% set bucket = env_var('RAW_STORAGE_CONTAINER', env_var('DATA_BUCKET', env_var('MINIO_BUCKET', 'data-lake-ipea'))) %}
        {{ return('s3://' ~ bucket ~ '/' ~ layer ~ '/' ~ table) }}
    {% elif backend == 'adls' %}
        {% set account = env_var('AZURE_STORAGE_ACCOUNT') %}
        {% set container = env_var('RAW_STORAGE_CONTAINER', env_var('AZURE_STORAGE_CONTAINER', env_var('DATA_CONTAINER', 'data-lake-ipea'))) %}
        {{ return('abfss://' ~ container ~ '@' ~ account ~ '.dfs.core.windows.net/' ~ layer ~ '/' ~ table) }}
    {% elif backend == 'gcs' %}
        {% set bucket = env_var('RAW_STORAGE_CONTAINER', env_var('DATA_BUCKET', env_var('GCS_BUCKET'))) %}
        {{ return('gs://' ~ bucket ~ '/' ~ layer ~ '/' ~ table) }}
    {% else %}
        {{ exceptions.raise_compiler_error("Unsupported OBJECT_STORAGE '" ~ backend ~ "'. Expected one of: s3, adls, gcs.") }}
    {% endif %}
{% endmacro %}

{% macro source_path(source_name, table_name) %}
    {{ return(storage_uri(source_name, table_name)) }}
{% endmacro %}
