{% macro normaliza_documento(col) %}
{#-
  Normaliza CPF/CNPJ para chave de junção: mantém só dígitos, vazio -> NULL.
  Sempre retorna text. CPF anonimizado do PNAB (com '*') vira NULL de dígitos
  aqui; a flag de "não casável" é derivada à parte via `documento_anonimizado`.
-#}
    nullif(regexp_replace(coalesce({{ col }}, ''), '[^0-9]', '', 'g'), '')
{% endmacro %}


{% macro documento_anonimizado(col) %}
{#- true quando o documento vem mascarado (contém '*'), logo não casável. -#}
    (coalesce({{ col }}, '') like '%*%')
{% endmacro %}
