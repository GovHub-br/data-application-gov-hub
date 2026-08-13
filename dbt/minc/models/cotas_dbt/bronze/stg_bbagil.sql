{{ config(enabled=false) }}
-- DESABILITADO até a extração bbágil rodar (bsc_pnab.fato_bbagil ainda não existe:
-- a DAG extracao_bbagil_dag falha na auth SCA — SCA_TOKEN_URL vazia no .env).
-- Quando o fato existir e o .env estiver corrigido, trocar para enabled=true.
--
-- Lado-valor VIA BANCO (BB Gestão Ágil / SERPRO): o valor PAGO ao beneficiário
-- final, não o repasse a entes. Fonte = bsc_pnab.fato_bbagil (gerado pela DAG
-- extracao_bbagil_dag do colega). Grão da fonte: (ente, documento) já agregado,
-- já filtrado (só débito p/ beneficiário, sem repasse entre entes públicos, ≥R$375).
--
-- POR QUE ISSO IMPORTA: o fct_pagamentos_elegiveis (lado das listas de contemplados)
-- mede valor RECEBIDO pelos entes (~R$2,7bi ≈ painel "recebido"). O bbágil mede o
-- valor GASTO/pago às pessoas (~R$447mi painel "gasto") — o denominador correto p/ cotas.
--
-- documento_beneficiario_bbagil vem REAL (não mascarado) → casa por CPF/CNPJ com
-- perfil_agentes_normalizado via a MESMA macro normaliza_documento (maçã com maçã).
select
    ente_bbagil,
    {{ normaliza_documento('documento_beneficiario_bbagil') }}          as identificador_unico,
    documento_beneficiario_bbagil                                        as documento_raw,
    (documento_beneficiario_bbagil like '%*%')                           as chave_anonimizada,
    {{ parse_valor('valor_transacao_total_bbagil') }}                    as valor_pago_num
from {{ source('bbagil', 'fato_bbagil') }}
where {{ normaliza_documento('documento_beneficiario_bbagil') }} is not null
   or {{ parse_valor('valor_transacao_total_bbagil') }} is not null
