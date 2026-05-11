import logging
import yaml
import pandas as pd
import re
import psycopg2
import unicodedata
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_ibge import ClienteIBGE
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval=get_dynamic_schedule("quilombolas_indigenas_censo_dag"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "owner": "Data Engineering Team",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["quilombolas", "indigenas", "censo_demografico", "ibge"],
)
def quilombolas_indigenas_censo_dag() -> None:

    @task
    def listar_arquivos_ftp() -> list:
        config_str = Variable.get(
            "ibge_censo_qi_config",
            default_var='{"database": "Quilombolas_e_Indigenas_por_sexo_e_idade_Resultados_do_universo"}',
        )
        config = yaml.safe_load(config_str)
        tema_base = config.get("database")

        pastas_alvo = [
            "Apendices/xlsx",
            "Tabelas_de_resultados/xlsx",
            "Tabelas_selecionadas/xlsx",
        ]
        todos_arquivos_encontrados = []

        for pasta in pastas_alvo:
            caminho_subpasta = f"{tema_base}/{pasta}"
            api_ftp = ClienteIBGE(database=caminho_subpasta)
            arquivos_da_pasta = api_ftp.listar_arquivos_alvo()
            if arquivos_da_pasta:
                for arq in arquivos_da_pasta:
                    if (
                        arq.endswith(".xlsx")
                        and not arq.startswith("~")
                        and "indice" not in arq.lower()
                    ):
                        todos_arquivos_encontrados.append(f"{pasta}/{arq}")

        return todos_arquivos_encontrados

    @task
    def processar_arquivo_ibge(arquivo: str) -> None:
        schema_destino = "censo_demografico"
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        MAPA_NOMES = {
            "q_i_s_i_apendice_01": (
                "Apêndice 1. Territórios Quilombolas citados nos acervos do Incra "
                "(geometrias indisponíveis)"
            ),
            "q_i_s_i_apendice_02": (
                "Apêndice 2. Terras Indígenas adicionadas entre o Censo 2010 e o "
                "Censo 2022"
            ),
            "q_i_s_i_apendice_03": (
                "Apêndice 3. Terras Indígenas por UF adicionadas entre o Censo 2010 e o "
                "Censo 2022"
            ),
            "q_i_s_i_apendice_04": (
                "Apêndice 4. Terras Indígenas comparáveis entre os Censos 2010 e 2022"
            ),
            "q_i_s_i_apendice_05": (
                "Apêndice 5. Terras Indígenas por UF comparáveis entre os Censos 2010 "
                "e 2022"
            ),
            "q_i_s_i_tabela_de_resultado_01": (
                "Tabela de resultado 1. Territórios Quilombolas oficialmente "
                "delimitados por status fundiário - Brasil - 2022"
            ),
            "q_i_s_i_tabela_de_resultado_02": (
                "Tabela de resultado 2. Territórios Quilombolas por UF segundo "
                "status fundiário - 2022"
            ),
            "q_i_s_i_tabela_de_resultado_03": (
                "Tabela de resultado 3. Pessoas residentes em Terras Indígenas "
                "- Brasil - 2010/2022"
            ),
            "q_i_s_i_tabela_de_resultado_04": (
                "Tabela de resultado 4. Pessoas residentes em Terras Indígenas "
                "por UF - 2010/2022"
            ),
            "q_i_s_i_tabela_de_resultado_05": (
                "Tabela de resultado 5. Variação população indígena até 17 anos "
                "por grupos de idade - Brasil - 2010/2022"
            ),
            "q_i_s_i_tabela_de_resultado_06": (
                "Tabela de resultado 6. Variação população indígena até 17 anos "
                "por TI e UF - Brasil - 2010/2022"
            ),
            "q_i_s_i_tabela_de_resultado_07": (
                "Tabela de resultado 7. Variação absoluta da idade mediana indígena "
                "por TI comparáveis - Brasil - 2010/2022"
            ),
            "q_i_s_i_tabela_de_resultado_08": (
                "Tabela de resultado 8. Variação absoluta da idade mediana indígena "
                "por TI por UF - Brasil - 2010/2022"
            ),
            "q_i_s_i_tabela_de_resultado_09": (
                "Tabela de resultado 9. Variação população indígena 60 anos ou mais "
                "- Brasil - 2010/2022"
            ),
            "q_i_s_i_tabela_de_resultado_10": (
                "Tabela de resultado 10. Variação população indígena 60 anos ou mais "
                "por UF - 2010/2022"
            ),
            "q_i_s_i_tabela_01": (
                "Tabela 1 - Pessoas residentes em Territórios Quilombolas por "
                "sexo e UF - Brasil – 2022"
            ),
            "q_i_s_i_tabela_02": (
                "Tabela 2 - Pessoas residentes em territórios quilombolas por "
                "idade e UF - Brasil – 2022"
            ),
            "q_i_s_i_tabela_03": (
                "Tabela 3 - Pessoas residentes em terras indígenas por sexo e "
                "UF - Brasil - 2010 e 2022"
            ),
            "q_i_s_i_tabela_04": (
                "Tabela 4 - Pessoas residentes em terras indígenas por idade e "
                "UF - Brasil - 2010 e 2022"
            ),
            "q_i_s_i_tabela_05": (
                "Tabela 5 - População residente total e quilombola por sexo e "
                "localização - Brasil – 2022"
            ),
            "q_i_s_i_tabela_06": (
                "Tabela 6 - População residente total e quilombola por idade e "
                "localização - Brasil – 2022"
            ),
            "q_i_s_i_tabela_07": (
                "Tabela 7 - População residente total e indígena por sexo e "
                "localização - Brasil – 2022"
            ),
            "q_i_s_i_tabela_08": (
                "Tabela 8 - População residente total e indígena por idade e "
                "localização - Brasil – 2022"
            ),
            "q_i_s_i_tabela_09": (
                "Tabela 9 - População residente total e quilombola por sexo e "
                "localização - Municípios – 2022"
            ),
            "q_i_s_i_tabela_10": (
                "Tabela 10 - População residente total e indígena por sexo e "
                "localização - Municípios – 2022"
            ),
            "q_i_s_i_tabela_11": (
                "Tabela 11 - População residente total e quilombola por idade e "
                "localização - Municípios – 2022"
            ),
            "q_i_s_i_tabela_12": (
                "Tabela 12 - População residente total e indígena por idade e "
                "localização - Municípios – 2022"
            ),
            "q_i_s_i_tabela_13": (
                "Tabela 13 - Razão de sexo da população total e quilombola por "
                "grupos de idade - Brasil – 2022"
            ),
            "q_i_s_i_tabela_14": (
                "Tabela 14 - Razão de sexo da população total e indígena por "
                "grupos de idade - Brasil – 2022"
            ),
            "q_i_s_i_tabela_15": (
                "Tabela 15 - Razão de sexo da população quilombola por idade e "
                "localização - Brasil – 2022"
            ),
            "q_i_s_i_tabela_16": (
                "Tabela 16 - Razão de sexo da população indígena por idade e "
                "localização - Brasil – 2022"
            ),
            "q_i_s_i_tabela_17": (
                "Tabela 17 - Razão de sexo da população total e quilombola por "
                "localização - Municípios – 2022"
            ),
            "q_i_s_i_tabela_18": (
                "Tabela 18 - Razão de sexo da população total e indígenas por "
                "localização - Municípios – 2010/2022"
            ),
            "q_i_s_i_tabela_19": (
                "Tabela 19 - Índice de envelhecimento da população total e "
                "quilombola por localização - Brasil – 2022"
            ),
            "q_i_s_i_tabela_20": (
                "Tabela 20 - Índice de envelhecimento da população total e "
                "indígena por localização - Brasil – 2010/2022"
            ),
            "q_i_s_i_tabela_21": (
                "Tabela 21 - Índice de envelhecimento da população total e "
                "quilombola por localização - Municípios – 2022"
            ),
            "q_i_s_i_tabela_22": (
                "Tabela 22 - Índice de envelhecimento da população total e "
                "indígena por localização - Municípios – 2010/2022"
            ),
            "q_i_s_i_tabela_23": (
                "Tabela 23 - Idade mediana da população residente total e "
                "quilombola - Brasil – 2022"
            ),
            "q_i_s_i_tabela_24": (
                "Tabela 24 - Idade mediana da população residente total e "
                "indígena - Brasil – 2010/2022"
            ),
            "q_i_s_i_tabela_25": (
                "Tabela 25 - Idade mediana da população residente total e "
                "quilombola - Municípios – 2022"
            ),
            "q_i_s_i_tabela_26": (
                "Tabela 26 - Idade mediana da população residente total e "
                "indígena - Municípios – 2022"
            ),
            "q_i_s_i_tabela_27": (
                "Tabela 27 - População total, quilombola e não quilombola na "
                "Amazônia Legal por sexo – 2022"
            ),
            "q_i_s_i_tabela_28": (
                "Tabela 28 - População total, indígena e não indígena na "
                "Amazônia Legal por sexo – 2022"
            ),
            "q_i_s_i_tabela_29": (
                "Tabela 29 - População residente total e quilombola na Amazônia "
                "Legal por idade e UF – Brasil – 2022"
            ),
            "q_i_s_i_tabela_30": (
                "Tabela 30 - População residente total e indígena na Amazônia "
                "Legal por idade e UF – Brasil – 2022"
            ),
            "q_i_s_i_tabela_31": (
                "Tabela 31 - Envelhecimento, idade mediana e razão de sexo "
                "quilombola Amazônia Legal – 2022"
            ),
            "q_i_s_i_tabela_32": (
                "Tabela 32 - Envelhecimento, idade mediana e razão de sexo "
                "indígena Amazônia Legal – 2022"
            ),
        }

        config_str = Variable.get(
            "ibge_censo_qi_config",
            default_var='{"database": "Quilombolas_e_Indigenas_por_sexo_e_idade_Resultados_do_universo"}',
        )
        config = yaml.safe_load(config_str)
        api_ftp = ClienteIBGE(database=config.get("database"))

        buffer = api_ftp.obter_conteudo_arquivo(arquivo)
        if not buffer:
            raise ValueError(f"Falha ao baixar {arquivo}")

        excel_file = pd.ExcelFile(buffer)
        abas_validas = [
            a
            for a in excel_file.sheet_names
            if not any(
                x in a.lower() for x in ["gráfico", "grafico", "nota", "índice", "indice"]
            )
        ]
        aba_alvo = abas_validas[0] if abas_validas else excel_file.sheet_names[0]
        df_aba = excel_file.parse(aba_alvo, header=None)

        clean_file = arquivo.split("/")[-1].split(".")[0].lower()
        match_num = re.search(r"\d+", clean_file)
        if match_num:
            num = match_num.group().zfill(2)
            if "apendice" in clean_file:
                table_name = f"q_i_s_i_apendice_{num}"
            elif "resultado" in clean_file:
                table_name = f"q_i_s_i_tabela_de_resultado_{num}"
            elif "tabela" in clean_file:
                table_name = f"q_i_s_i_tabela_{num}"
            else:
                table_name = f"q_i_s_i_outros_{num}"
        else:
            table_name = f"q_i_s_i_{clean_file[:20]}"

        if "apendice" in clean_file:
            mascara_num = df_aba.apply(
                lambda r: r.dropna().astype(str).str.strip().ne("").sum() > 2, axis=1
            )
        else:
            mascara_num = df_aba.apply(
                lambda r: pd.to_numeric(r, errors="coerce").notna().sum() > 1, axis=1
            )

        if not mascara_num.any():
            logging.warning(f"Arquivo ignorado (sem estrutura tabular): {arquivo}")
            return

        idx_dados = mascara_num.idxmax()
        linhas_cabecalho = df_aba.iloc[:idx_dados].copy().ffill(axis=1)

        nomes_colunas, dict_comentarios, contagem_nomes = [], {}, {}
        for col_idx in range(len(df_aba.columns)):
            pedacos = [
                str(linhas_cabecalho.iloc[r, col_idx]).split(" - ")[-1].strip()
                for r in range(len(linhas_cabecalho))
                if str(linhas_cabecalho.iloc[r, col_idx]).lower() != "nan"
                and len(linhas_cabecalho.iloc[r].dropna().unique()) > 1
            ]
            nome_bruto = "_".join(pedacos) if pedacos else f"coluna_{col_idx}"
            c_limpo = re.sub(
                r"[^\w_]",
                "",
                "".join(
                    c
                    for c in unicodedata.normalize("NFD", nome_bruto)
                    if unicodedata.category(c) != "Mn"
                )
                .lower()
                .replace(" ", "_")
                .replace("-", "_"),
            )
            nome_limpo = f"ano_{c_limpo}" if c_limpo and c_limpo[0].isdigit() else c_limpo
            nome_final = nome_limpo[:55]
            if nome_final in contagem_nomes:
                contagem_nomes[nome_final] += 1
                nome_final = f"{nome_final}_{contagem_nomes[nome_final]}"
            else:
                contagem_nomes[nome_final] = 0
            nomes_colunas.append(nome_final)
            dict_comentarios[nome_final] = " > ".join(pedacos)

        df = df_aba.iloc[idx_dados:].copy()
        df.columns = nomes_colunas
        df = df.dropna(subset=[df.columns[0]])
        dados = df.to_dict(orient="records")

        try:
            with psycopg2.connect(postgres_conn_str) as conn:
                with conn.cursor() as cursor:

                    cursor.execute(
                        f"DROP TABLE IF EXISTS {schema_destino}.{table_name} CASCADE;"
                    )
                conn.commit()

            db.insert_data(data=dados, table_name=table_name, schema=schema_destino)

            with psycopg2.connect(postgres_conn_str) as conn:
                with conn.cursor() as cursor:
                    nome_real = MAPA_NOMES.get(table_name, f"Censo 2022 - {table_name}")
                    cursor.execute(
                        f"COMMENT ON TABLE {schema_destino}.{table_name} IS %s;",
                        (nome_real,),
                    )
                    for col, desc in dict_comentarios.items():
                        if col in df.columns:
                            cursor.execute(
                                f"COMMENT ON COLUMN {schema_destino}.{table_name}.{col} IS %s;",
                                (desc,),
                            )
                conn.commit()
            logging.info(f"Sucesso: {table_name}")
        except Exception as e:
            logging.error(f"Erro: {table_name} -> {e}")
            raise

    lista_de_arquivos = listar_arquivos_ftp()
    processar_arquivo_ibge.expand(arquivo=lista_de_arquivos)


dag_instance = quilombolas_indigenas_censo_dag()
