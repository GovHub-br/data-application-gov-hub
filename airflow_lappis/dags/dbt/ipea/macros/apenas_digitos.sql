{#
    Limpa um texto e deixa só os dígitos (CPF, CEP, agência). Vazio vira nulo.
    Uso: {{ apenas_digitos("cpfpensionista") }} as cpf_pensionista
#}
{% macro apenas_digitos(coluna) -%}
    regexp_replace(nullif(trim({{ coluna }}), ''), '[^0-9]', '', 'g')
{%- endmacro %}
