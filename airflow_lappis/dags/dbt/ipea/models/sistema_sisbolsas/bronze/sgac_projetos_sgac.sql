{{ config(materialized="table") }}

with
    projetos_sgac as (
        select
            ({{ safe_numeric("id_interno_item", 18, 0) }})::bigint as id_interno_item,
            ({{ safe_numeric("id", 18, 0) }})::bigint as id,
            {{ safe_text("titulo") }} as titulo,
            {{ safe_text("entidades_externas") }} as entidades_externas,
            {{ extract_jsonb_key_values("instrumento", "Value") }} as instrumento,
            ({{ safe_numeric("instrumento_id", 18, 0) }})::bigint as instrumento_id,
            {{ extract_jsonb_key_values("diretoria_responsavel", "Value") }}
            as diretoria_responsavel,
            ({{ safe_numeric("diretoria_responsavel_id", 18, 0) }})::bigint
            as diretoria_responsavel_id,
            {{ clean_sharepoint_html("objeto") }} as objeto,
            {{ safe_date("data_inicio") }} as data_inicio,
            {{ safe_date("data_vencimento") }} as data_vencimento,
            {{ safe_numeric("total_de_recursos", 18, 2) }} as total_de_recursos,
            {{ safe_text("numero_do_proc") }} as numero_do_proc,
            {{ extract_jsonb_key_values("coordenador", "DisplayName") }} as coordenador,
            {{ sharepoint_jsonb("coordenador") }} as coordenador_json,
            {{ sharepoint_jsonb("coordenador_claims") }} as coordenador_claims,
            {{ extract_jsonb_key_values("nacionalidade", "Value") }} as nacionalidade,
            {{ sharepoint_jsonb("nacionalidade_id") }} as nacionalidade_id,
            {{ safe_numeric("recursos_orcament_x00", 18, 2) }}
            as recursos_orcamentarios,
            {{ safe_numeric("recursos_orcament_x0", 18, 2) }}
            as recursos_nao_orcamentarios,
            {{ extract_jsonb_key_values("status", "Value") }} as status,
            ({{ safe_numeric("status_id", 18, 0) }})::bigint as status_id,
            {{ extract_jsonb_key_values("eixo_tematico", "Value") }} as eixo_tematico,
            {{ sharepoint_jsonb("eixo_tematico_id") }} as eixo_tematico_id,
            {{ extract_jsonb_key_values("predecessores", "Value") }} as predecessores,
            {{ sharepoint_jsonb("predecessores_id") }} as predecessores_id,
            {{ extract_jsonb_key_values("prioridade", "Value") }} as prioridade,
            ({{ safe_numeric("prioridade_id", 18, 0) }})::bigint as prioridade_id,
            {{ clean_sharepoint_html("justificativa") }} as justificativa,
            {{ clean_sharepoint_html("objetivo_s_ge") }} as objetivo_s_ge,
            {{ extract_jsonb_key_values("equipe_tecnica", "DisplayName") }} as equipe_tecnica,
            {{ sharepoint_jsonb("equipe_tecnica") }} as equipe_tecnica_json,
            {{ sharepoint_jsonb("equipe_tecnica_claims") }} as equipe_tecnica_claims,
            {{ safe_text("codigo") }} as codigo,
            {{ extract_jsonb_key_values("unidades_envolvidas", "Value") }}
            as unidades_envolvidas,
            {{ sharepoint_jsonb("unidades_envolvidas_id") }} as unidades_envolvidas_id,
            {{ clean_sharepoint_html("historico_observa_x0") }} as historico_observa_x0,
            {{ extract_jsonb_key_values("a_solicitacao", "Value") }} as a_solicitacao,
            {{ sharepoint_jsonb("a_solicitacao_id") }} as a_solicitacao_id,
            {{ safe_timestamp("modificado") }} as modificado,
            {{ safe_timestamp("criado") }} as criado,
            {{ extract_jsonb_key_values("autor", "DisplayName") }} as autor,
            {{ extract_jsonb_key_values("autor", "Email", fallback_to_text=false) }}
            as autor_email,
            {{ sharepoint_jsonb("autor") }} as autor_json,
            {{ safe_text("autor_claims") }} as autor_claims,
            {{ extract_jsonb_key_values("editor", "DisplayName") }} as editor,
            {{ extract_jsonb_key_values("editor", "Email", fallback_to_text=false) }}
            as editor_email,
            {{ sharepoint_jsonb("editor") }} as editor_json,
            {{ safe_text("editor_claims") }} as editor_claims,
            {{ safe_text("link") }} as link,
            {{ safe_text("nome") }} as nome,
            {{ safe_text("termos_aditivos") }} as termos_aditivos,
            {{ clean_sharepoint_html("equipe") }} as equipe,
            {{ safe_numeric("percentual_concluido", 18, 4) }} as percentual_concluido,
            {{ clean_sharepoint_html("corpo") }} as corpo,
            {{ clean_sharepoint_html("fiscal_e_substituto") }} as fiscal_e_substituto,
            {{ safe_text("numero_siafi") }} as numero_siafi,
            {{ extract_jsonb_key_values("atribuido_a", "DisplayName") }} as atribuido_a,
            {{ safe_text("atribuido_a_claims") }} as atribuido_a_claims,
            {{ safe_timestamp("dt_ingest") }} as dt_ingest
        from {{ source("sgac", "projetos_sgac") }}
    )

select *
from projetos_sgac
