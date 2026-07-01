select *
from {{ source("conjuntura_gold", "gold_balancos_empresas_vendas") }}
