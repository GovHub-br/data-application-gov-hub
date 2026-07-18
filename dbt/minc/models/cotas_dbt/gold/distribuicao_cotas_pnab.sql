-- Cotas PNAB (SOMENTE PNAB). 3 grupos: negra 25%, indígena 10%, PCD 5%.
-- SEM cota territorial (o flag_territorio_vulneravel só existe p/ LPG — agente
-- com cidade/uf; no PNAB o lado-valor vem das listas/bbágil sem localização).
-- Lógica na macro distribuicao_cotas (compartilhada com LPG).
--
-- RESSALVA (Diag. 4): enquanto o lado-valor PNAB vier das listas de contemplados,
-- o denominador mede valor RECEBIDO (repasse a entes ~R$2,7bi), não o valor PAGO
-- a pessoas (~R$447mi). Quando o bbágil (fct_pagamentos_bbagil) estiver ativo, este
-- modelo deve passar a ler o valor pago real — ver plano_integracao_meta3.
{{ distribuicao_cotas('PNAB', incluir_territorio=false) }}
