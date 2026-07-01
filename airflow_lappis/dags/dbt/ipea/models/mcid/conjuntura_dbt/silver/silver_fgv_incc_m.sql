select *
from {{ source("conjuntura_silver", "silver_fgv_incc_m") }}
