select *
from {{ source("conjuntura_gold", "gold_saldo_caderneta_poupanca") }}
