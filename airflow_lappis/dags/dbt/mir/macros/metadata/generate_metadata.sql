{% macro get_model_metadata() %}
{#
    Esta macro retorna os metadados do modelo atual.
    Pode ser usada em post-hooks para registrar metadados automaticamente.
#}
    SELECT
        '{{ this.schema }}' AS schema_name,
        '{{ this.name }}' AS table_name,
        '{{ this.database }}' AS database_name,
        ('{{ run_started_at }}'::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS dt_transform,
        '{{ invocation_id }}' AS run_id
{% endmacro %}


{% macro register_model_metadata() %}
{#
    Esta macro registra os metadados do modelo em uma tabela central.
    Deve ser usada como post-hook nos modelos que deseja rastrear.
    
    Uso no dbt_project.yml:
    models:
      ipea:
        +post-hook:
          - "{{ register_model_metadata() }}"
#}

    INSERT INTO {{ target.database }}.metadata.models_metadata (
        schema_name,
        table_name,
        database_name,
        dt_transform,
        run_id
    )
    VALUES (
        '{{ this.schema }}',
        '{{ this.name }}',
        '{{ this.database }}',
        ('{{ run_started_at }}'::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'),
        '{{ invocation_id }}'
    )
    ON CONFLICT (schema_name, table_name)
    DO UPDATE SET
        dt_transform = EXCLUDED.dt_transform,
        run_id = EXCLUDED.run_id;

{% endmacro %}
