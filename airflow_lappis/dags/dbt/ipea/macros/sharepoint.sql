{% macro clean_sharepoint_text(column_name) -%}
    case
        when {{ column_name }} is null then null
        when nullif(trim({{ column_name }}::text), '') is null then null
        when upper(trim({{ column_name }}::text)) = 'NAN' then null
        else trim({{ column_name }}::text)
    end
{%- endmacro %}

{% macro clean_sharepoint_html(column_name) -%}
    nullif(
        trim(
            regexp_replace(
                regexp_replace(
                    replace(
                        replace({{ clean_sharepoint_text(column_name) }}, '&nbsp;', ' '),
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

{% macro sharepoint_reference_value(column_name) -%}
    case
        when {{ clean_sharepoint_text(column_name) }} like ('{' || '%')
        then ({{ clean_sharepoint_text(column_name) }})::jsonb ->> 'Value'
        else {{ clean_sharepoint_text(column_name) }}
    end
{%- endmacro %}

{% macro sharepoint_reference_values(column_name) -%}
    case
        when {{ clean_sharepoint_text(column_name) }} like ('[' || '%')
        then (
            select string_agg(element ->> 'Value', '; ' order by ordinality)
            from
                jsonb_array_elements(({{ clean_sharepoint_text(column_name) }})::jsonb)
                with ordinality as elements(element, ordinality)
        )
        else {{ clean_sharepoint_text(column_name) }}
    end
{%- endmacro %}

{% macro sharepoint_user_display_names(column_name) -%}
    case
        when {{ clean_sharepoint_text(column_name) }} like ('[' || '%')
        then (
            select string_agg(element ->> 'DisplayName', '; ' order by ordinality)
            from
                jsonb_array_elements(({{ clean_sharepoint_text(column_name) }})::jsonb)
                with ordinality as elements(element, ordinality)
        )
        when {{ clean_sharepoint_text(column_name) }} like ('{' || '%')
        then ({{ clean_sharepoint_text(column_name) }})::jsonb ->> 'DisplayName'
        else {{ clean_sharepoint_text(column_name) }}
    end
{%- endmacro %}

{% macro sharepoint_user_emails(column_name) -%}
    case
        when {{ clean_sharepoint_text(column_name) }} like ('[' || '%')
        then (
            select string_agg(element ->> 'Email', '; ' order by ordinality)
            from
                jsonb_array_elements(({{ clean_sharepoint_text(column_name) }})::jsonb)
                with ordinality as elements(element, ordinality)
            where nullif(element ->> 'Email', '') is not null
        )
        when {{ clean_sharepoint_text(column_name) }} like ('{' || '%')
        then ({{ clean_sharepoint_text(column_name) }})::jsonb ->> 'Email'
        else null
    end
{%- endmacro %}

{% macro sharepoint_jsonb(column_name) -%}
    case
        when {{ clean_sharepoint_text(column_name) }} like ('[' || '%')
            or {{ clean_sharepoint_text(column_name) }} like ('{' || '%')
        then ({{ clean_sharepoint_text(column_name) }})::jsonb
        else null
    end
{%- endmacro %}
