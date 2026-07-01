select *
from {{ source("conjuntura_gold", "gold_indicadores_financiamento_pf") }}
