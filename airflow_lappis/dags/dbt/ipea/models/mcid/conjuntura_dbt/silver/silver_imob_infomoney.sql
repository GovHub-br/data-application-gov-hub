select *
from {{ source("conjuntura_silver", "silver_imob_infomoney") }}
