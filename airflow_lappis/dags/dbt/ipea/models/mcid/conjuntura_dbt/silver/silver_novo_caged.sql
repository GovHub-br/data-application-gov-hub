select *
from {{ source("conjuntura_silver", "silver_novo_caged") }}
