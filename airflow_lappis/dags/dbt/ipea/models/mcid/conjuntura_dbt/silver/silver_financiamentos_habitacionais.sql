select *
from {{ source("conjuntura_silver", "silver_financiamentos_habitacionais") }}
