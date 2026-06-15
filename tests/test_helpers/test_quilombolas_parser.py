import pandas as pd

from quilombolas_parser import (
    PREFIXO_TABELA,
    construir_nome_tabela,
    construir_nome_tabela_indice,
    deduplicar_colunas,
    normalizar_nome_coluna,
    parsear_arquivo_indice,
    preparar_registros_insercao,
    processar_chunk_excel,
    resolver_chave_primaria,
)


def test_construir_nome_tabela_prefixo() -> None:
    assert construir_nome_tabela("Tabela_de_resultado_02.xlsx") == (
        f"{PREFIXO_TABELA}tabela_de_resultado_02"
    )


def test_construir_nome_tabela_indice() -> None:
    assert construir_nome_tabela_indice("Tabelas_de_resultados") == (
        f"{PREFIXO_TABELA}indice_tabelas_de_resultados"
    )


def test_normalizar_nome_coluna_remove_acentos() -> None:
    nome = normalizar_nome_coluna("Taxa de alfabetização (%)", 0)
    assert "alfabetizacao" in nome
    assert "porcentagem" in nome


def test_deduplicar_colunas() -> None:
    assert deduplicar_colunas(["total", "total", "total"]) == [
        "total",
        "total_1",
        "total_2",
    ]


def test_parsear_arquivo_indice() -> None:
    conteudo = (
        "Tabela 1 - População residente - Brasil - 2022\n"
        "Tabela 2 - População por UF - 2022\n"
    )
    chunk = parsear_arquivo_indice(conteudo, "Tabelas_de_resultados")
    assert chunk.table_name == f"{PREFIXO_TABELA}indice_tabelas_de_resultados"
    assert len(chunk.df) == 2
    assert chunk.df.iloc[0]["numero"] == "1"
    assert "População residente" in chunk.df.iloc[0]["descricao"]


def test_parsear_arquivo_indice_sem_hifen() -> None:
    """Índice IBGE pode usar espaços duplos em vez de hífen."""
    conteudo = "Tabela  3  População por sexo - 2022\n"
    chunk = parsear_arquivo_indice(conteudo, "Tabelas_selecionadas")
    assert chunk.df.iloc[0]["numero"] == "3"
    assert "População por sexo" in chunk.df.iloc[0]["descricao"]


def test_localizar_linha_titulo_tabela_complementar() -> None:
    from quilombolas_parser import _localizar_linha_titulo

    df = pd.DataFrame(
        [
            ["Censo Demográfico 2022"],
            ["Tabela complementar 1 - Alfabetização - 2022"],
            ["Brasil", "100"],
        ]
    )
    assert _localizar_linha_titulo(df) == 1


def test_processar_chunk_excel_apendice() -> None:
    """Simula layout do Apêndice 1 (tabela textual sem valores numéricos)."""
    linhas = [
        ["Censo Demográfico 2022", None],
        ["Quilombolas: tema", None],
        ["Apêndice 1 - Territórios citados - Brasil - 2022", None],
        ["Estado", "Território Quilombola"],
        ["PA", "Cuxiu"],
        ["PA", "Guajarauna"],
    ]
    df_raw = pd.DataFrame(linhas)
    chunk = processar_chunk_excel(
        df_raw, "Apendice_01.xlsx", "Apendices/xlsx", "Apendice 1"
    )
    assert chunk is not None
    assert chunk.table_name == f"{PREFIXO_TABELA}apendice_01"
    assert list(chunk.df.columns)[0] == "estado"
    assert len(chunk.df) == 2
    assert chunk.table_comment.startswith("Territórios")


def test_resolver_chave_primaria_territorios_por_uf() -> None:
    """Vários territórios na mesma UF exigem PK além das 3 primeiras colunas."""
    df = pd.DataFrame(
        [
            ["Titulado", "11", "RO", "Rondônia", "11001", "TQ A", "100", "50"],
            ["Titulado", "11", "RO", "Rondônia", "11280", "TQ B", "200", "80"],
        ],
        columns=[
            "status_fundiario",
            "unidade_da_federacao_codigo",
            "unidade_da_federacao_sigla",
            "unidade_da_federacao_nome",
            "territorio_quilombola_codigo",
            "territorio_quilombola_nome",
            "populacao_residente_total",
            "populacao_residente_quilombola",
        ],
    )
    pk = resolver_chave_primaria(df)
    assert "territorio_quilombola_codigo" in pk
    assert len(pk) <= 5
    assert df.drop_duplicates(subset=pk).shape[0] == len(df)


def test_preparar_registros_deduplica_por_pk() -> None:
    from quilombolas_parser import ChunkProcessado

    chunk = ChunkProcessado(
        df=pd.DataFrame(
            [["PA", "Cuxiu"], ["PA", "Cuxiu"]],
            columns=["estado", "territorio_quilombola"],
        ),
        table_name=f"{PREFIXO_TABELA}apendice_01",
        table_comment="teste",
        primary_key=["estado", "territorio_quilombola"],
    )
    registros = preparar_registros_insercao(chunk)
    assert len(registros) == 1


def test_processar_chunk_excel_cabecalho_triplo() -> None:
    """Simula cabeçalho hierárquico de tabela complementar."""
    linhas = [
        ["Censo Demográfico 2022"] * 3,
        ["Quilombolas: tema"] * 3,
        [None] * 3,
        ["Tabela complementar 1 - Alfabetização por região - 2022"] * 3,
        [None] * 3,
        ["Região", "População 15+", "População 15+"],
        [None, "Total", "Quilombolas"],
        [None, None, None],
        ["Brasil", "100", "10"],
        ["Norte", "50", "5"],
    ]
    df_raw = pd.DataFrame(linhas)
    chunk = processar_chunk_excel(
        df_raw,
        "Tabela_complementar_01.xlsx",
        "Tabelas_selecionadas/xlsx",
        "Tabela complementar 1",
    )
    assert chunk is not None
    assert "regiao" in chunk.df.columns[0]
    assert len(chunk.df) == 2
    assert "total" in chunk.col_comments.get(chunk.df.columns[1], "").lower()
