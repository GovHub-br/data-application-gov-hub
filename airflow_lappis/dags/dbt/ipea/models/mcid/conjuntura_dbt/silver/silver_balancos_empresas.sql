select *
from {{ source("conjuntura_silver", "silver_balancos_empresas") }}
