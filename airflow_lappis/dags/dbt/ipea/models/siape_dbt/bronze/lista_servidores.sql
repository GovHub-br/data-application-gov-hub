WITH lista_servidores AS (
	SELECT
		cpf,
		TO_DATE(dataultimatransacao, 'DDMMYYYY') AS dt_ultima_transacao,
		coduorg AS cod_uorg
	FROM {{ source('siape', 'lista_servidores') }}
)

SELECT * FROM lista_servidores
