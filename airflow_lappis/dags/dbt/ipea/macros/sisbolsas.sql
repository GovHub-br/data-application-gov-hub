{#
    Normaliza o código de situação do bolsista vindo do SQL Server, que pode
    chegar float-ificado na ingestão (ex.: '1.0' em vez de '1').
#}
{% macro sisbolsas_codigo_situacao_bolsista(coluna) %}
    regexp_replace(btrim({{ coluna }}), '[.]0+$', '')
{% endmacro %}


{#
    Regra única de "bolsista ativo" para os modelos silver do Sisbolsas,
    baseada em `tb_situacao_bolsista`:
      1 = Ativo
      3 = Encerramento pendente (bolsa encerrada, mas o bolsista ainda tem
          pendências; incluída por inferência, a confirmar com o Ipea)
    Com essa regra, a origem em 08/10/2026 deu 252 bolsistas ativos, o mesmo
    número informado pelo Ipea em 11/09/2026. Situações 2 (Suspenso), 4 e 8
    (Encerrado) e as de declaração (5 a 7) ficam de fora.
#}
{% macro sisbolsas_bolsista_ativo(coluna) %}
    coalesce({{ sisbolsas_codigo_situacao_bolsista(coluna) }} in ('1', '3'), false)
{% endmacro %}
