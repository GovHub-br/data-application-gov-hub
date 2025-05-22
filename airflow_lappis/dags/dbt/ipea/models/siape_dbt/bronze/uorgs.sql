SELECT
    cast(codigo AS INT) AS codigo,
    dataultimatransacao,
    nome
FROM {{ source('siape', 'lista_uorgs') }}