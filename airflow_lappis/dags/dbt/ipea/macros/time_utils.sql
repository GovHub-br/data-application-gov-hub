{% macro brasilia_now_iso() %}
    to_char(
        (current_timestamp at time zone 'America/Sao_Paulo'),
        'YYYY-MM-DD"T"HH24:MI:SS.US"-03:00"'
    )
{% endmacro %}