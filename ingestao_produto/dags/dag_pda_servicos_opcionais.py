import os

import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, dag, task

TA_SERVICO_OPCIONAL = Asset("STG_PRODUTO.TA_SERVICO_OPCIONAL")
RL_REGS_PLANO_SERVICO_OPCIONAL = Asset("STG_PRODUTO.RL_REGS_PLANO_SERVICO_OPCIONAL")
TB_REGS_PLANO = Asset("STG_PRODUTO.TB_REGS_PLANO")

dag_id = "ingestao_produto"
project_dir = f"{os.getcwd()}/git_repo/dags/{dag_id}"
file_path = f"{os.getcwd()}/files/resultado.csv"
include_dir = f"{project_dir}/include"


@dag(
    dag_id="pda_servicos_opcionais",
    schedule=[TA_SERVICO_OPCIONAL, RL_REGS_PLANO_SERVICO_OPCIONAL, TB_REGS_PLANO],
    start_date=pendulum.datetime(2025, 11, 12, tz="UTC"),
    catchup=False,
    template_searchpath=include_dir,
    tags=["oracle", "pda", "produtos"],
)
def pda_servicos_opcionais():

    inicio = EmptyOperator(task_id="inicio")

    consulta = SQLExecuteQueryOperator(
        task_id="consulta_produtos",
        conn_id="oracle_bi_pda",
        sql="/sql/qy_produto.sql",
    )

    @task
    def grava_dados(resultados):
        from pathlib import Path

        import pandas as pd

        caminho_csv = Path(file_path)
        caminho_csv.parent.mkdir(parents=True, exist_ok=True)
        # Converte em DataFrame
        df = pd.DataFrame(resultados)
        # Salva como CSV
        df.to_csv(f"{caminho_csv}", index=False, encoding="utf-8")

        if caminho_csv.exists():
            print("O arquivo foi gerado!")
        else:
            print("O arquivo não foi encontrado!")

    upload_sftp = SFTPOperator(
        task_id="upload_sftp",
        ssh_conn_id="sftp_default",
        local_filepath=file_path,
        remote_filepath="/FTP/TESTE_AIRFLOW/PDA/servicos_opcionais.csv",
        operation="put",
        create_intermediate_dirs=True,
    )

    send_email = EmailOperator(
        task_id="send_email",
        to="fabricio.francisco@ans.gov.br",
        from_email="airflow@ans.gov.br",
        subject="[AIRFLOW] - Teste envio de arquivo como anexo",
        html_content="""
                                <h3>Segue em anexo o resultado da consulta.</h3>
                                <p>Gerado automaticamente pelo Airflow.</p>
                                """,
        files=[file_path],
    )

    fim = EmptyOperator(task_id="fim")

    resultado = consulta.output
    grava = grava_dados(resultado)

    inicio >> grava >> upload_sftp >> send_email >> fim


pda_servicos_opcionais()
