select *
from {{ source("conjuntura_silver", "silver_fgts_financiamentos_habitacionais") }}
