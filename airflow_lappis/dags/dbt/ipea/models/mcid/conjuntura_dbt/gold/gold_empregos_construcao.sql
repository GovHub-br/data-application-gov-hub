select *
from {{ source("conjuntura_gold", "gold_empregos_construcao") }}
