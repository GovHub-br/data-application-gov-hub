select
    diretoria_responsavel as diretoria,
    instrumento,
    objeto,
    entidades_externas,
    data_inicio as inicio,
    data_vencimento as conclusao,
    recursos_orcamentarios as recursos_externos,
    recursos_nao_orcamentarios as recursos_ipea,
    numero_do_proc as numero_do_processo,
    coordenador,
    nullif(
        trim(
            regexp_replace(
                regexp_replace(
                    replace(
                        replace(fiscal_e_substituto, '&nbsp;', ' '),
                        '&#58;',
                        ':'
                    ),
                    '<[^>]*>',
                    ' ',
                    'g'
                ),
                '\s+',
                ' ',
                'g'
            )
        ),
        ''
    ) as fiscal_e_substituto,
    status as situacao
from {{ ref("sgac_projetos_sgac") }}
order by conclusao, diretoria, instrumento, entidades_externas
