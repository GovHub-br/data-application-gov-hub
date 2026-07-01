select *
from {{ source("conjuntura_silver", "silver_ibge_pib_construcao_civil") }}
