select *
from {{ source("conjuntura_silver", "silver_abecip_novos_financiamentos_imobiliarios") }}
