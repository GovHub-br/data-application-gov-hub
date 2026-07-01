select *
from {{ source("conjuntura_gold", "gold_cbic_lancamentos_regiao") }}
