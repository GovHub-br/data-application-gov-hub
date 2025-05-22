

SELECT
    cast(codigo AS INT) AS codigo,
    TO_DATE(dataultimatransacao, 'DDMMYYYY') AS dataultimatransacao,
    nome
FROM {{ source('siape', 'lista_uorgs') }}