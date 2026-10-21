{% macro conceder_usage_dinamico() %}
{#
    Concede USAGE no schema do role somente-leitura (de_postgres
    via DB_DW_READONLY_ROLE) para todo schema que o próprio dbt gerencia.

    Por que isso é seguro: USAGE só permite "entrar" no schema, não dá
    SELECT em nada. Quem decide o que o role pode LER continua sendo só
    o +grants de cada model (bronze: select: [], silver/gold: select:
    [role]) -- dar USAGE largo aqui não expõe bronze em lugar nenhum,
    nem na listagem via information_schema (que filtra por privilégio
    de tabela, não de schema).

    A lista de schemas vem do graph.nodes (os models reais do projeto),
    não de uma lista fixa -- então um domínio novo já entra sozinho no
    próximo dbt run, sem precisar editar esse arquivo.
#}
{% if execute %}
    {% set role = env_var('DB_DW_READONLY_ROLE', 'de_postgres') %}
    {% set schemas_configurados = graph.nodes.values()
        | selectattr('resource_type', 'equalto', 'model')
        | map(attribute='schema')
        | unique
        | list %}

    {#
        Nem todo schema declarado em +schema já existe de fato no banco
        -- um domínio pode estar configurado no dbt_project.yml mas nunca
        ter rodado com sucesso ainda (ex.: falta fonte de dado). Sem esse
        filtro, o GRANT quebra o dbt run inteiro por causa de um schema
        que nem foi criado.
    #}
    {% set schemas_existentes_query %}
        select schema_name from information_schema.schemata
        where schema_name in ({{ "'" ~ schemas_configurados | join("','") ~ "'" }})
    {% endset %}
    {% set resultado = run_query(schemas_existentes_query) %}
    {% set schemas_existentes = resultado.columns[0].values() if resultado is not none else [] %}

    {% for schema in schemas_configurados %}
        {% if schema in schemas_existentes %}
            {% do run_query('GRANT USAGE ON SCHEMA "' ~ schema ~ '" TO "' ~ role ~ '"') %}
            {{ log("USAGE concedido em " ~ schema ~ " para " ~ role, info=true) }}
        {% else %}
            {{ log("Pulando " ~ schema ~ " -- schema ainda não existe no banco", info=true) }}
        {% endif %}
    {% endfor %}
{% endif %}
{% endmacro %}
