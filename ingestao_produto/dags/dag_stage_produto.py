from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, dag, task_group

ROWS_CHUNK = 100_000

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
    "TA_SERVICO_OPCIONAL": [
        "CD_SERVICO_OPCIONAL",
        "DE_SERVICO_OPCIONAL",
        "NR_ORDEM_EXIBICAO",
    ],
    "RL_REGS_PLANO_SERVICO_OPCIONAL": [
        "ID_PLANO",
        "CD_SERVICO_OPCIONAL",
        "DE_SERVICO_OPCIONAL_OUTROS",
    ],
}


@dag(dag_id="dag_stage_produto")
def dag_stage_produto():

    inicio = EmptyOperator(task_id="inicio")

    @task_group(group_id="transferencia_dados_produto")
    def transferencia_dados_produto():
        from airflow.providers.oracle.operators.oracle import (
            OracleStoredProcedureOperator,
        )
        from airflow.providers.oracle.transfers.oracle_to_oracle import (
            OracleToOracleOperator,
        )

        for nome_tabela, colunas in tables_to_process.items():
            truncate = OracleStoredProcedureOperator(
                task_id=f"truncate_{nome_tabela.lower()}",
                oracle_conn_id=ORACLE_DST_CONN_ID,
                procedure="STG_PRODUTO.PR_TRUNCATE_STG_PRODUTO",
                parameters=[nome_tabela],
            )

            oracle_asset = Asset(name=f"{SCHEMA_DESTINO}.{nome_tabela}")

            ingest = OracleToOracleOperator(
                task_id=f"ingest_{nome_tabela.lower()}",
                oracle_destination_conn_id=ORACLE_DST_CONN_ID,
                destination_table=f"{SCHEMA_DESTINO}.{nome_tabela}",
                oracle_source_conn_id=ORACLE_ORG_CONN_ID,
                source_sql=f"SELECT {', '.join(colunas)} FROM {SCHEMA_ORIGEM}.{nome_tabela}",
                rows_chunk=ROWS_CHUNK,
                outlets=[oracle_asset],
            )

            truncate >> ingest

    transferencia_dados_produto = transferencia_dados_produto()

    fim = EmptyOperator(task_id="fim")

    inicio >> transferencia_dados_produto >> fim


dag_stage_produto()
