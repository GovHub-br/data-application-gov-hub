select *
from {{ source("conjuntura_silver", "silver_ibge_pnadc_rendimento_construcao") }}
