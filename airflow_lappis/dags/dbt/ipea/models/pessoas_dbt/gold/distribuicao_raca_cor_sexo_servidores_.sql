-- Distribuição de servidores por raça/cor e sexo do servidor
select
    nome_cor,
    sum(case when nome_sexo = 'FEMININO' then 1 else 0 end) * -1 as feminino,
    sum(case when nome_sexo = 'MASCULINO' then 1 else 0 end) as masculino,
    nome_situacao_funcional,
    max(dt_ingest) as dt_ingest
from {{ ref("hierarquia") }}
group by nome_cor, nome_situacao_funcional
order by nome_cor
