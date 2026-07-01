select *
from {{ source("conjuntura_silver", "silver_bacen_financiamentos_imobiliarios") }}
