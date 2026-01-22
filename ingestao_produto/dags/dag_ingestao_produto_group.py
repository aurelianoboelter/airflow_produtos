import os

import pyarrow
import pyarrow.compute as pc
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, AssetAlias, Metadata, dag, task, task_group

ROWS_CHUNK = 100_000

DAG_DIR = os.path.dirname(os.path.abspath(__file__))

ORACLE_ORG_CONN_ID = "oracle_trans_ingestao_produto"
ORACLE_DST_CONN_ID = "oracle_bi_ingestao_produto"

SCHEMA_ORIGEM = "DIPRO"
SCHEMA_DESTINO = "STG_PRODUTO"

tables_to_process = {
    "TB_REGS_PLANO": [
        "ID_PLANO",
        "CD_OPERADORA",
        "NR_PLANO",
        "NM_PLANO",
        "CD_TIPO_PLANO",
        "CD_PADRAO_ACOMODACAO",
        "CD_OPCAO_POS_ESTABELECIDO",
        "CD_PARTICIPACAO_FINC_PJ",
        "LG_FUNDO_COPARTICIPACAO",
        "LG_FUNDO_FRANQUIA",
        "LG_VINCULO_BENE_ATIVO",
        "LG_VINCULO_BENE_INATIVO",
        "LG_SEM_VINCULO_BENE",
        "DT_INICIO_COMERCIALIZACAO",
        "DT_FIM_COMERCIALIZACAO",
        "CD_REGIAO_ATUACAO",
        "DT_REGISTRO",
        "LG_TOTAL_LIVRE_ESCOLHA",
        "ID_TIPO_MODALIDADE_FINM",
        "ID_TIPO_CONTRATACAO",
        "ID_TIPO_SEGMENTACAO",
        "ID_TIPO_ABRANGENCIA",
        "CD_SITUACAO_PLANO",
        "DT_SITUACAO",
        "ID_INDICE_REAJUSTE",
        "ID_SOLICITACAO_REGISTRO",
        "DT_CRIACAO",
        "ID_PLANO_ORIGEM",
        "ID_TIPO_REGISTRO",
        "ID_PLANO_ORIGEM_TRFN",
        "LG_IN09",
        "ID_TIPO_ORIGEM",
        "ID_SUBTIPO_ORIGEM",
        "ID_PLANO_TAXA",
    ],
    #    "TA_SERVICO_OPCIONAL": ["CD_SERVICO_OPCIONAL", "DE_SERVICO_OPCIONAL", "NR_ORDEM_EXIBICAO"],
    #    "RL_REGS_PLANO_SERVICO_OPCIONAL": ["ID_PLANO", "CD_SERVICO_OPCIONAL", "DE_SERVICO_OPCIONAL_OUTROS"],
}


@dag(
    dag_id="dag_ingestao_produto_group",
    max_active_tasks=1,
    max_active_runs=1,
    template_searchpath=[os.path.join(DAG_DIR, "../include/sql")],
)
def dag_ingestao_produto_group():

    inicio = EmptyOperator(task_id="inicio")

    @task(outlets=[AssetAlias("my-task-outputs")])
    def ingest_table(tabela: str, colunas: list = None, asset: Asset = None):
        from airflow.providers.oracle.hooks.oracle import OracleHook

        if colunas is None:
            colunas = []
        colunas_sql = ", ".join(colunas)
        query_sql = f"SELECT {colunas_sql} FROM {SCHEMA_ORIGEM}.{tabela}"
        with OracleHook(oracle_conn_id=ORACLE_ORG_CONN_ID).get_conn() as src_conn:
            with OracleHook(oracle_conn_id=ORACLE_DST_CONN_ID).get_conn():
                rows = 0

                schema = pyarrow.schema(
                    [
                        ("NM_PLANO", pyarrow.string()),
                    ]
                )

                odf = src_conn.fetch_df_all(
                    statement=query_sql, requested_schema=schema
                )

                tab = pyarrow.table(odf)

                print(f"Tab:{tab}")

                coluna = tab["NM_PLANO"]

                # 1. Calcula o comprimento de cada string (em caracteres ou bytes)
                tamanhos = pc.utf8_length(coluna)

                # 2. Cria uma máscara booleana para valores > 60
                mascara = pc.greater(tamanhos, 60)

                # 3. Filtra a tabela original
                resultado = tab.filter(mascara)

                print(f"Linhas encontradas: {resultado.num_rows}")

                print("Default Output Coluna:", coluna)

                print(f"Tabela {tabela} - registros:{rows}")

                # for odf in src_conn.fetch_df_batches(statement=query_sql, size=ROWS_CHUNK):
                #     dst_conn.direct_path_load(schema_name=SCHEMA_DESTINO,
                #                               table_name=tabela,
                #                               column_names=colunas,
                #                               data=odf,
                #                               batch_size=ROWS_CHUNK
                #     )
                #     rows += odf.num_rows()

        yield Metadata(asset, extra={"rows": rows}, alias=AssetAlias("my-task-outputs"))

    @task_group(group_id="transferencia_dados_produto")
    def transferencia_dados_produto():
        from airflow.providers.oracle.operators.oracle import (
            OracleStoredProcedureOperator,
        )

        for nome_tabela, colunas in tables_to_process.items():

            truncate = OracleStoredProcedureOperator(
                task_id=f"truncate_{nome_tabela.lower()}",
                oracle_conn_id=ORACLE_DST_CONN_ID,
                procedure="STG_PRODUTO.PR_TRUNCATE_STG_PRODUTO",
                parameters=[nome_tabela],
            )

            oracle_asset = Asset(name=f"{SCHEMA_DESTINO}.{nome_tabela}")

            ingest = ingest_table.override(
                task_id=f"ingest_{nome_tabela.lower()}", outlets=[oracle_asset]
            )(tabela=nome_tabela, colunas=colunas, asset=oracle_asset)

            truncate >> ingest

    transferencia_dados_produto = transferencia_dados_produto()

    fim = EmptyOperator(task_id="fim")

    inicio >> transferencia_dados_produto >> fim


dag_ingestao_produto_group()
