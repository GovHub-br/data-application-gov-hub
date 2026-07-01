select *
from {{ source("conjuntura_gold", "gold_pib_construcao_civil") }}
