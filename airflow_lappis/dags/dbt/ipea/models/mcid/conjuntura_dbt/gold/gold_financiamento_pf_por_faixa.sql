select *
from {{ source("conjuntura_gold", "gold_financiamento_pf_por_faixa") }}
