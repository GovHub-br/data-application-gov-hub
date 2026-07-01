select *
from {{ source("conjuntura_silver", "silver_ibge_sinapi") }}
