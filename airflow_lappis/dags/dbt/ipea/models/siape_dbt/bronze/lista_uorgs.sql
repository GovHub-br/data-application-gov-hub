
WITH lista_uorgs AS (
    SELECT
        cast(codigo AS INT) AS codigo,
        TO_DATE(dataultimatransacao, 'DDMMYYYY') AS dt_ultima_transacao,
        nome
    FROM {{ source('siape', 'lista_uorgs') }}
)

SELECT * FROM lista_uorgs
