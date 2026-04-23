{{ config(materialized="table") }}

with
    sisbolsas_tb_folha_pagamento as (
        select
            co_folha_pagamento::text as co_folha_pagamento,
            nu_solicitacao::text as nu_solicitacao,
            nu_mes::text as nu_mes,
            nu_ano::text as nu_ano,
            nu_lote::text as nu_lote,
            co_situacao_folha::text as co_situacao_folha,
            dt_criacao::text as dt_criacao,
            nu_total_integral::text as nu_total_integral,
            nu_total_parcial::text as nu_total_parcial,
            co_usuario_criacao::text as co_usuario_criacao,
            co_usuario_homologacao::text as co_usuario_homologacao,
            dt_homologacao::text as dt_homologacao
        from {{ source("sisbolsas", "tb_folha_pagamento") }}
    )

select *
from sisbolsas_tb_folha_pagamento
