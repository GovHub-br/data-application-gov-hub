select *
from {{ source("mcid", "evolucao_financeira") }}
