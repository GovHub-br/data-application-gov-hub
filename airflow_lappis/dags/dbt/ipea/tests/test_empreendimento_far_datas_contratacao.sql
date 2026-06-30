select *
from {{ ref("empreendimento_far") }}
where data_contratacao is not null
    and coalesce(
        data_contratacao > data_conclusao,
        true
    )
    and coalesce(
        data_contratacao > data_entrega,
        true
    )
    and (
        data_conclusao is not null
        or data_entrega is not null
    )
