select *
from {{ source("conjuntura_gold", "gold_precos_construcao") }}
