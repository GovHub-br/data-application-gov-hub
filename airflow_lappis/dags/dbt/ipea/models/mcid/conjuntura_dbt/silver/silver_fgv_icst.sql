select *
from {{ source("conjuntura_silver", "silver_fgv_icst") }}
