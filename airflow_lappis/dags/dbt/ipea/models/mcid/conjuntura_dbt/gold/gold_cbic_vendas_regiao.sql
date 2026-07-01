select *
from {{ source("conjuntura_gold", "gold_cbic_vendas_regiao") }}
