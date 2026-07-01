select *
from {{ source("conjuntura_gold", "gold_indices_mercado_imobiliario") }}
