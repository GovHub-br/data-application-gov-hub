select *
from {{ source("conjuntura_gold", "gold_abecip_novos_financiamentos_imobiliarios") }}
