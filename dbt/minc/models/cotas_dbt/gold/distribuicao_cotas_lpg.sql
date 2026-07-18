-- Cotas LPG (SOMENTE LPG). 4 grupos: negra 25%, indígena 10%, PCD 5%,
-- território vulnerabilizado 20%. Valor ponderado por ano, veredito vs meta.
-- Lógica na macro distribuicao_cotas (compartilhada com PNAB, sem duplicar SQL).
{{ distribuicao_cotas('LPG', incluir_territorio=true) }}
