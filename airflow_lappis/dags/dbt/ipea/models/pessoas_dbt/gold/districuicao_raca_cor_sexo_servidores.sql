select
    nome_cor,
    nome_situacao_funcional,
    sum(case when nome_sexo = 'FEMININO' then 1 else 0 end) * -1 as feminino,
    sum(case when nome_sexo = 'MASCULINO' then 1 else 0 end) as masculino
from {{ ref("hierarquia") }}
group by nome_cor, nome_situacao_funcional
order by nome_cor
