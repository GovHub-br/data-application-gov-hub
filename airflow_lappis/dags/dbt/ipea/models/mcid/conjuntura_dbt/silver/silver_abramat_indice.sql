select *
from {{ source("conjuntura_silver", "silver_abramat_indice") }}
