-- Distribuição de servidores por gênero
select
    nome_sexo as genero,
    count(*) as quantidade_servidores,
    count(*) * 1.0 / sum(count(*)) over () as percentual_distribuicao
from {{ ref("servidores_completos") }}
group by nome_sexo
order by percentual_distribuicao desc
