select *
from {{ source("conjuntura_silver", "silver_abecip_poupanca_sbpe") }}
