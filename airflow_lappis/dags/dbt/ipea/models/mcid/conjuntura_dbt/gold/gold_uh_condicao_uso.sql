select *
from {{ source("conjuntura_gold", "gold_uh_condicao_uso") }}
