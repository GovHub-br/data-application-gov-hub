select *
from {{ source("conjuntura_silver", "silver_cbic_lancamentos_vendas") }}
