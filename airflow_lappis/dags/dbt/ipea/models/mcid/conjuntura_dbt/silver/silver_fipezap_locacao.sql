select *
from {{ source("conjuntura_silver", "silver_fipezap_locacao") }}
