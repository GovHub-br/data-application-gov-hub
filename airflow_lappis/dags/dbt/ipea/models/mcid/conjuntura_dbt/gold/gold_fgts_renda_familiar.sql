select *
from {{ source("conjuntura_gold", "gold_fgts_renda_familiar") }}
