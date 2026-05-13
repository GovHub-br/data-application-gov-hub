-- Distribuição de servidores por raça/cor
select nome_cor as cor_raca, count(nome_cor) as quantidade_servidores, max(dt_ingest) as dt_ingest
from {{ ref("servidores_completos") }}
group by nome_cor
order by quantidade_servidores desc
