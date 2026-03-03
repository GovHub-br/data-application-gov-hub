-- Distribuição de servidores por raça/cor e sexo do servidor
SELECT
    nome_cor,
    SUM(CASE WHEN nome_sexo = 'FEMININO' THEN 1 ELSE 0 END) * -1 AS feminino,
    SUM(CASE WHEN nome_sexo = 'MASCULINO' THEN 1 ELSE 0 END) AS masculino,
    nome_situacao_funcional,
    max(dt_ingest) as dt_ingest
FROM {{ ref("hierarquia") }}
GROUP BY nome_cor, nome_situacao_funcional
ORDER BY nome_cor
