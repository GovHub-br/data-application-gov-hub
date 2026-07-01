select *
from {{ source("conjuntura_silver", "silver_abecip_sbpe_financiamentos_habitacionais") }}
