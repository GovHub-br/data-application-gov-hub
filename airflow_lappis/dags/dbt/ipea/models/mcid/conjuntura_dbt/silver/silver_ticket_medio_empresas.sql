select *
from {{ source("conjuntura_silver", "silver_ticket_medio_empresas") }}
