{{ config(materialized="table") }}

with
    projetos_sgac as (
        select
            {{ clean_sharepoint_text("odata_etag") }} as odata_etag,
            ({{ safe_numeric("id_interno_item", 18, 0) }})::bigint as id_interno_item,
            ({{ safe_numeric("id", 18, 0) }})::bigint as id,
            {{ clean_sharepoint_text("titulo") }} as titulo,
            {{ clean_sharepoint_text("entidades_externas") }} as entidades_externas,
            {{ sharepoint_reference_value("instrumento") }} as instrumento,
            ({{ safe_numeric("instrumento_id", 18, 0) }})::bigint as instrumento_id,
            {{ sharepoint_reference_value("diretoria_responsavel") }}
            as diretoria_responsavel,
            ({{ safe_numeric("diretoria_responsavel_id", 18, 0) }})::bigint
            as diretoria_responsavel_id,
            {{ clean_sharepoint_html("objeto") }} as objeto,
            {{ safe_date("data_inicio") }} as data_inicio,
            {{ safe_date("data_vencimento") }} as data_vencimento,
            {{ safe_numeric("total_de_recursos", 18, 2) }} as total_de_recursos,
            {{ clean_sharepoint_text("numero_do_proc") }} as numero_do_proc,
            {{ sharepoint_user_display_names("coordenador") }} as coordenador,
            {{ sharepoint_jsonb("coordenador") }} as coordenador_json,
            {{ clean_sharepoint_text("coordenador_tipo_odata") }}
            as coordenador_tipo_odata,
            {{ sharepoint_jsonb("coordenador_claims") }} as coordenador_claims,
            {{ clean_sharepoint_text("coordenador_claims_tipo_odata") }}
            as coordenador_claims_tipo_odata,
            {{ sharepoint_reference_values("nacionalidade") }} as nacionalidade,
            {{ clean_sharepoint_text("nacionalidade_tipo_odata") }}
            as nacionalidade_tipo_odata,
            {{ sharepoint_jsonb("nacionalidade_id") }} as nacionalidade_id,
            {{ clean_sharepoint_text("nacionalidade_id_tipo_odata") }}
            as nacionalidade_id_tipo_odata,
            {{ safe_numeric("recursos_orcament_x00", 18, 2) }}
            as recursos_orcamentarios,
            {{ safe_numeric("recursos_orcament_x0", 18, 2) }}
            as recursos_nao_orcamentarios,
            {{ sharepoint_reference_value("status") }} as status,
            ({{ safe_numeric("status_id", 18, 0) }})::bigint as status_id,
            {{ sharepoint_reference_values("eixo_tematico") }} as eixo_tematico,
            {{ clean_sharepoint_text("eixo_tematico_tipo_odata") }}
            as eixo_tematico_tipo_odata,
            {{ sharepoint_jsonb("eixo_tematico_id") }} as eixo_tematico_id,
            {{ clean_sharepoint_text("eixo_tematico_id_tipo_odata") }}
            as eixo_tematico_id_tipo_odata,
            {{ sharepoint_reference_values("predecessores") }} as predecessores,
            {{ clean_sharepoint_text("predecessores_tipo_odata") }}
            as predecessores_tipo_odata,
            {{ sharepoint_jsonb("predecessores_id") }} as predecessores_id,
            {{ clean_sharepoint_text("predecessores_id_tipo_odata") }}
            as predecessores_id_tipo_odata,
            {{ sharepoint_reference_value("prioridade") }} as prioridade,
            ({{ safe_numeric("prioridade_id", 18, 0) }})::bigint as prioridade_id,
            {{ clean_sharepoint_html("justificativa") }} as justificativa,
            {{ clean_sharepoint_html("objetivo_s_ge") }} as objetivo_s_ge,
            {{ sharepoint_user_display_names("equipe_tecnica") }} as equipe_tecnica,
            {{ sharepoint_jsonb("equipe_tecnica") }} as equipe_tecnica_json,
            {{ clean_sharepoint_text("equipe_tecnica_tipo_odata") }}
            as equipe_tecnica_tipo_odata,
            {{ sharepoint_jsonb("equipe_tecnica_claims") }} as equipe_tecnica_claims,
            {{ clean_sharepoint_text("equipe_tecnica_claims_tipo_odata") }}
            as equipe_tecnica_claims_tipo_odata,
            {{ clean_sharepoint_text("codigo") }} as codigo,
            {{ sharepoint_reference_values("unidades_envolvidas") }}
            as unidades_envolvidas,
            {{ clean_sharepoint_text("unidades_envolvidas_tipo_odata") }}
            as unidades_envolvidas_tipo_odata,
            {{ sharepoint_jsonb("unidades_envolvidas_id") }} as unidades_envolvidas_id,
            {{ clean_sharepoint_text("unidades_envolvidas_id_tipo_odata") }}
            as unidades_envolvidas_id_tipo_odata,
            {{ clean_sharepoint_html("historico_observa_x0") }} as historico_observa_x0,
            {{ sharepoint_reference_values("a_solicitacao") }} as a_solicitacao,
            {{ clean_sharepoint_text("a_solicitacao_tipo_odata") }}
            as a_solicitacao_tipo_odata,
            {{ sharepoint_jsonb("a_solicitacao_id") }} as a_solicitacao_id,
            {{ clean_sharepoint_text("a_solicitacao_id_tipo_odata") }}
            as a_solicitacao_id_tipo_odata,
            {{ safe_timestamp("modificado") }} as modificado,
            {{ safe_timestamp("criado") }} as criado,
            {{ sharepoint_user_display_names("autor") }} as autor,
            {{ sharepoint_user_emails("autor") }} as autor_email,
            {{ sharepoint_jsonb("autor") }} as autor_json,
            {{ clean_sharepoint_text("autor_claims") }} as autor_claims,
            {{ sharepoint_user_display_names("editor") }} as editor,
            {{ sharepoint_user_emails("editor") }} as editor_email,
            {{ sharepoint_jsonb("editor") }} as editor_json,
            {{ clean_sharepoint_text("editor_claims") }} as editor_claims,
            {{ clean_sharepoint_text("identificador") }} as identificador,
            {{ safe_boolean("eh_pasta") }} as eh_pasta,
            {{ sharepoint_jsonb("miniatura") }} as miniatura,
            {{ clean_sharepoint_text("link") }} as link,
            {{ clean_sharepoint_text("nome") }} as nome,
            {{ clean_sharepoint_text("nome_arquivo_com_extensao") }}
            as nome_arquivo_com_extensao,
            {{ clean_sharepoint_text("caminho") }} as caminho,
            {{ clean_sharepoint_text("caminho_completo") }} as caminho_completo,
            case
                when {{ clean_sharepoint_text("tipo_conteudo") }} like ('{' || '%')
                then ({{ clean_sharepoint_text("tipo_conteudo") }})::jsonb ->> 'Name'
                else {{ clean_sharepoint_text("tipo_conteudo") }}
            end as tipo_conteudo,
            {{ clean_sharepoint_text("tipo_conteudo_id") }} as tipo_conteudo_id,
            {{ safe_boolean("possui_anexos") }} as possui_anexos,
            {{ safe_numeric("numero_versao", 18, 2) }} as numero_versao,
            ({{ safe_numeric("aprovacao", 18, 0) }})::bigint as aprovacao,
            {{ clean_sharepoint_text("termos_aditivos") }} as termos_aditivos,
            {{ clean_sharepoint_html("equipe") }} as equipe,
            {{ safe_numeric("percentual_concluido", 18, 4) }} as percentual_concluido,
            {{ clean_sharepoint_html("corpo") }} as corpo,
            {{ clean_sharepoint_html("fiscal_e_substituto") }} as fiscal_e_substituto,
            {{ clean_sharepoint_text("numero_siafi") }} as numero_siafi,
            {{ sharepoint_user_display_names("atribuido_a") }} as atribuido_a,
            {{ clean_sharepoint_text("atribuido_a_claims") }} as atribuido_a_claims,
            {{ safe_timestamp("dt_ingest") }} as dt_ingest
        from {{ source("sgac", "projetos_sgac") }}
    )

select *
from projetos_sgac
