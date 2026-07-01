select *
from {{ source("conjuntura_gold", "gold_ibge_pnad_construcao") }}
