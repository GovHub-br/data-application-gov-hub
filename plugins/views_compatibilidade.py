"""Views que mantem os nomes antigos de tabela funcionando apos a
reorganizacao do banco para o modelo do documento.

Contexto: a ingestao passou a gravar nos nomes da especificacao
(``programa_minc``, ``plano_acao_minc``, ``extrato_bbagil``, as seis
``planilha_*``). Os ~20 modelos dbt de ``cotas_dbt``/``agentes_dbt`` leem os
nomes antigos, e reescrever todos eles neste momento colidiria de frente com
a branch da Meta 5, que esta mexendo nos mesmos arquivos. Estas views sao a
ponte: o banco fica conforme e o dbt continua funcionando sem alteracao.

Sao criadas pela ingestao, nao pelo dbt, de proposito -- para o dbt sao
*sources*, e uma source precisa existir antes da primeira execucao dele.

Sao temporarias: quando os modelos forem repontados para as tabelas novas
(item de backlog), este modulo e as views saem juntos.
"""

import logging

import schemas_minc as schemas
from cliente_postgres import ClientPostgresDB

# Nome antigo -> tabela nova. Cada linha das tabelas de planilha guarda em
# ``tabela_origem`` o nome antigo de onde ela teria ido, e e isso que
# reconstitui a tabela original como uma fatia da consolidada.
_VIEWS_PLANILHA: dict[str, tuple[str, str]] = {
    nome: (schemas.TABELA_PLANILHA_DADOS_LPG, nome)
    for nome in (
        "lpg_dados_pessoa_fisica",
        "lpg_dados_pessoa_fisica_audiovisual",
        "lpg_dados_pessoa_fisica_multicultur",
        "lpg_dados_pessoa_juridica",
        "lpg_dados_pessoa_juridica_audiovisu",
        "lpg_dados_pessoa_juridica_multicult",
        "lpg_dados_coletivos",
        "lpg_dados_grupo_coletivo",
        "lpg_dados_grupo_coletivo_audiovisua",
        "lpg_dados_grupo_coletivo_multicultu",
        "lpg_dados_instrumentos",
        "lpg_dados_instrumentos_2",
        "lpg_dados_instrumentos_2_2",
        "lpg_dados_instrumentos_publicos",
    )
}
_VIEWS_PLANILHA.update(
    {
        "lpg_contemplados": (
            schemas.TABELA_PLANILHA_CONTEMPLADOS_LPG,
            "lpg_contemplados",
        ),
        "lpg_editais": (schemas.TABELA_PLANILHA_EDITAIS_LPG, "lpg_editais"),
        "raw_pnab_lista_contemplados_geral": (
            schemas.TABELA_PLANILHA_CONTEMPLADOS_PNAB,
            "raw_pnab_lista_contemplados_geral",
        ),
        "raw_pnab_lista_contemplados_pncv": (
            schemas.TABELA_PLANILHA_CONTEMPLADOS_PNAB,
            "raw_pnab_lista_contemplados_pncv",
        ),
        "raw_pnab_acoes_gerais": (
            schemas.TABELA_PLANILHA_EDITAIS_PNAB,
            "raw_pnab_acoes_gerais",
        ),
        "raw_pnab_acoes_cultura_viva": (
            schemas.TABELA_PLANILHA_EDITAIS_PNAB,
            "raw_pnab_acoes_cultura_viva",
        ),
        "pnab_pessoas": (schemas.TABELA_PLANILHA_DADOS_PNAB, "pnab_pessoas"),
        "pnab_organizacoes": (schemas.TABELA_PLANILHA_DADOS_PNAB, "pnab_organizacoes"),
    }
)

# Renomes simples: mesma granularidade, so mudou o nome da tabela.
_VIEWS_RENOMEADAS: dict[str, tuple[str, str]] = {
    "raw_programas": (schemas.SCHEMA_TRANSFEREGOV, schemas.TABELA_PROGRAMA),
    "raw_planos_acao": (schemas.SCHEMA_TRANSFEREGOV, schemas.TABELA_PLANO_ACAO),
    "raw_planos_acao_dado_bancario": (
        schemas.SCHEMA_TRANSFEREGOV,
        schemas.TABELA_PLANO_ACAO_DADO_BANCARIO,
    ),
    "raw_bbagil_extrato_transacoes": (schemas.SCHEMA_BBAGIL, schemas.TABELA_EXTRATO),
    "raw_bbagil_subtransacoes": (schemas.SCHEMA_BBAGIL, schemas.TABELA_SUBTRANSACAO),
}


def _colunas(db: ClientPostgresDB, schema: str, tabela: str) -> list[str]:
    linhas = db.execute_query(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_schema = '{schema}' AND table_name = '{tabela}' "
        "ORDER BY ordinal_position"
    )
    return [coluna for (coluna,) in linhas]


def _sql_view_planilha(
    db: ClientPostgresDB, nome_view: str, tabela_destino: str, tabela_origem: str
) -> str | None:
    colunas = _colunas(db, schemas.SCHEMA_RELATORIO_GESTAO, tabela_destino)
    if not colunas:
        return None

    # Os modelos dbt extraem o id do anexo com
    # ``substring(id_anexo from 'anexo_([0-9]+)')``, porque o valor antigo era
    # o nome do arquivo ("anexo_123_relatorio"). Agora ``id_anexo`` guarda so
    # o id -- a view devolve o formato antigo para os modelos continuarem
    # casando.
    projecao = ", ".join(
        "('anexo_' || id_anexo) AS id_anexo" if coluna == "id_anexo" else f'"{coluna}"'
        for coluna in colunas
    )

    return (
        f"CREATE VIEW {schemas.SCHEMA_TRANSFEREGOV}.{nome_view} AS "
        f"SELECT {projecao} FROM {schemas.SCHEMA_RELATORIO_GESTAO}.{tabela_destino} "
        f"WHERE tabela_origem = '{tabela_origem}'"
    )


def criar_views_compatibilidade(db: ClientPostgresDB) -> int:
    """(Re)cria as views de compatibilidade e devolve quantas foram criadas.

    Usa DROP + CREATE em vez de ``CREATE OR REPLACE`` porque as tabelas de
    planilha ganham colunas ao longo do tempo, e o Postgres so aceita
    substituir uma view quando a lista de colunas nao muda.

    Falha de uma view nao derruba as demais: sao um apoio de transicao, nao
    a carga em si.
    """
    criadas = 0

    for nome_view, (tabela_destino, tabela_origem) in _VIEWS_PLANILHA.items():
        sql = _sql_view_planilha(db, nome_view, tabela_destino, tabela_origem)
        if sql is None:
            logging.info(
                "[views_compatibilidade] %s.%s ainda não existe — view %s não criada",
                schemas.SCHEMA_RELATORIO_GESTAO,
                tabela_destino,
                nome_view,
            )
            continue

        criadas += _executar(db, f"{schemas.SCHEMA_TRANSFEREGOV}.{nome_view}", sql)

    for nome_view, (schema_destino, tabela_destino) in _VIEWS_RENOMEADAS.items():
        if not _colunas(db, schema_destino, tabela_destino):
            logging.info(
                "[views_compatibilidade] %s.%s ainda não existe — view %s não criada",
                schema_destino,
                tabela_destino,
                nome_view,
            )
            continue

        sql = (
            f"CREATE VIEW {schema_destino}.{nome_view} AS "
            f"SELECT * FROM {schema_destino}.{tabela_destino}"
        )
        criadas += _executar(db, f"{schema_destino}.{nome_view}", sql)

    logging.info("[views_compatibilidade] %d views de compatibilidade criadas", criadas)
    return criadas


def _executar(db: ClientPostgresDB, nome_qualificado: str, sql_create: str) -> int:
    try:
        db.execute_statement(f"DROP VIEW IF EXISTS {nome_qualificado}")
        db.execute_statement(sql_create)
        return 1
    except Exception as exc:
        logging.warning(
            "[views_compatibilidade] Falha ao criar a view %s: %s",
            nome_qualificado,
            exc,
        )
        return 0
