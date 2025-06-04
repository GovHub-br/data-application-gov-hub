select
    coalesce(nome_cargo, 'N/A') as cargo_efetivo,
    coalesce(nome_sexo, 'N/A') as genero,
    coalesce(nome_situacao_funcional, 'N/A') as situacao_funcional,
    coalesce(uf_uorg, 'N/A') as localidade_uf,
    count(distinct cpf) as quantidade_servidores
from {{ ref("servidores_detalhados") }}
group by 1, 2, 3, 4
