{#
    Retorna uma expressão booleana que diz se a coluna traz um CPF válido:
    11 dígitos, não todos iguais e com os dois dígitos verificadores corretos.
    A coluna deve conter apenas dígitos (como os modelos bronze do SIAPE já entregam).
    Nulo ou fora do formato retorna false. O CASE garante que os casts para int só
    rodam quando a coluna tem 11 dígitos (o Postgres não garante a ordem do AND).
    Uso: where not {{ esocial_cpf_valido("cpf") }}
#}
{% macro esocial_cpf_valido(coluna) -%}
    case
        when
            {{ coluna }} is null
            or {{ coluna }} !~ '^[0-9]{11}$'
            or {{ coluna }} ~ '^([0-9])\1{10}$'
        then false
        else
            (
                (
                    {%- for i in range(1, 10) %}
                        substr({{ coluna }}, {{ i }}, 1)::int
                        * {{ 11 - i }}{{ " +" if not loop.last }}
                    {%- endfor %}
                )
                * 10
                % 11
                % 10
            )
            = substr({{ coluna }}, 10, 1)::int
            and (
                (
                    {%- for i in range(1, 11) %}
                        substr({{ coluna }}, {{ i }}, 1)::int
                        * {{ 12 - i }}{{ " +" if not loop.last }}
                    {%- endfor %}
                )
                * 10
                % 11
                % 10
            )
            = substr({{ coluna }}, 11, 1)::int
    end
{%- endmacro %}
