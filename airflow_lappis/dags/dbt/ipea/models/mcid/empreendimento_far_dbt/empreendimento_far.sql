select *
from {{ source("mcid", "empreendimento_far") }}
