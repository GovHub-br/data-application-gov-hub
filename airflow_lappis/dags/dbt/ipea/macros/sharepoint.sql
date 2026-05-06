{% macro clean_sharepoint_html(column_name) -%}
    nullif(
        trim(
            regexp_replace(
                regexp_replace(
                    replace(
                        replace({{ safe_text(column_name) }}, '&nbsp;', ' '),
                        '&#160;',
                        ' '
                    ),
                    '<[^>]*>',
                    ' ',
                    'g'
                ),
                '\s+',
                ' ',
                'g'
            )
        ),
        ''
    )
{%- endmacro %}

{% macro extract_jsonb_key_values(
    column_name, key_name, fallback_to_text=true, separator='; '
) -%}
    case
        when {{ safe_text(column_name) }} like ('[' || '%')
        then (
            select
                string_agg(
                    element ->> '{{ key_name }}',
                    '{{ separator }}' order by ordinality
                )
            from
                jsonb_array_elements(({{ safe_text(column_name) }})::jsonb)
                with ordinality as elements(element, ordinality)
            where nullif(element ->> '{{ key_name }}', '') is not null
        )
        when {{ safe_text(column_name) }} like ('{' || '%')
        then ({{ safe_text(column_name) }})::jsonb ->> '{{ key_name }}'
        {% if fallback_to_text %}
        else {{ safe_text(column_name) }}
        {% else %}
        else null
        {% endif %}
    end
{%- endmacro %}

{% macro sharepoint_jsonb(column_name) -%}
    case
        when {{ safe_text(column_name) }} like ('[' || '%')
            or {{ safe_text(column_name) }} like ('{' || '%')
        then ({{ safe_text(column_name) }})::jsonb
        else null
    end
{%- endmacro %}
