import csv
import logging
import os

import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import (  # Airflow 2.x (substitui DummyOperator)
    EmptyOperator,
)
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.smtp.hooks.smtp import SmtpHook

# =============================================================================
# Configurações Gerais
# =============================================================================
ORACLE_CONN_ID = "oracle_trans_ingestao_produto"
MINIO_CONN_ID = "minio_conn"
MINIO_BUCKET = "bi-pda"
MINIO_EXPORT_PATH = "exports/teste_pda"
CSV_ENCODING = "cp1252"
CSV_SEPARATOR = ";"
TEMP_DATA_DIR = "/tmp"

# =============================================================================
# Configurações de Email (sem HTML gigante)
# =============================================================================
SMTP_CONN_ID = "smtp_default"  # ajuste para a sua conexão SMTP no Airflow
DEFAULT_FROM = "airflow@ans.gov.br"
DEFAULT_TO = [
    "aureliano.silva@ans.gov.br",
]
LOG_URL_REPLACE = {
    "old": "http://localhost:8080",
    "new": "http://airflow-ds.ans.gov.br:8080",
}


def _build_success_email(
    dag_id: str, task_id: str, ts: str, log_url: str, s3_path: str
):
    subject = "[Airflow - SUCESSO] Exportação PDA Minio concluída!"
    body = f"""
Olá,

A DAG {dag_id} foi executada com sucesso.

DAG: {dag_id}
Tarefa: {task_id}
Execução: {ts}

Arquivo gerado:
{s3_path}

Logs:
{log_url}

Mensagem automática - Sistema Airflow ANS
""".strip()
    return subject, body


@dag(
    dag_id="dag_processamento_pda_igr",
    start_date=pendulum.datetime(2025, 8, 29, tz="America/Sao_Paulo"),
    schedule=None,
    catchup=False,
    tags=["pda", "minio", "export", "email"],
)
def pipeline_pda_minio():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task(task_id="extrair_dados_oracle")
    def extrair_dados_oracle() -> str:
        context = get_current_context()
        ts_nodash = context["ts_nodash"]

        file_name = f"extracao_pda_{ts_nodash}.csv"
        local_file_path = os.path.join(TEMP_DATA_DIR, file_name)

        logging.info("Iniciando consulta Oracle. Destino local: %s", local_file_path)

        oracle_hook = OracleHook(oracle_conn_id=ORACLE_CONN_ID)

        total_rows = 0
        chunk_size = 5000

        with oracle_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select * from dm_fiscalizacao.vw_ir_pda_igr

                    """
                )

                headers = [desc[0].lower() for desc in cur.description]

                # escreve direto no disco (economiza memória)
                with open(local_file_path, "w", newline="", encoding=CSV_ENCODING) as f:
                    writer = csv.writer(
                        f,
                        delimiter=CSV_SEPARATOR,
                        quotechar='"',
                        quoting=csv.QUOTE_MINIMAL,
                    )
                    writer.writerow(headers)

                    while True:
                        rows = cur.fetchmany(chunk_size)
                        if not rows:
                            break
                        writer.writerows(rows)
                        total_rows += len(rows)

        logging.info(
            "Extração concluída: %s linhas salvas em %s", total_rows, local_file_path
        )
        return local_file_path

    @task(task_id="enviar_para_minio")
    def enviar_para_minio(local_file_path: str) -> str:
        file_name = os.path.basename(local_file_path)
        minio_key = f"{MINIO_EXPORT_PATH}/{file_name}"

        logging.info("Enviando para MinIO: %s/%s", MINIO_BUCKET, minio_key)

        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        s3_hook.load_file(
            filename=local_file_path,
            key=minio_key,
            bucket_name=MINIO_BUCKET,
            replace=True,
        )

        # limpeza do arquivo local temporário
        try:
            os.remove(local_file_path)
            logging.info("Arquivo local temporário removido: %s", local_file_path)
        except Exception as e:
            logging.warning(
                "Não foi possível remover arquivo local %s: %s", local_file_path, e
            )

        full_s3_path = f"s3://{MINIO_BUCKET}/{minio_key}"
        logging.info("Upload finalizado: %s", full_s3_path)
        return full_s3_path

    @task(task_id="enviar_email")
    def enviar_email(s3_path: str) -> None:
        """
        Envia email de sucesso no final do fluxo (sem callback).
        """
        context = get_current_context()
        dag_id = context["dag"].dag_id
        task_id = context["ti"].task_id
        ts = context["ts"]

        # Ajusta URL do log para ambiente correto
        log_url = context["ti"].log_url.replace(
            LOG_URL_REPLACE["old"], LOG_URL_REPLACE["new"]
        )

        subject, body = _build_success_email(
            dag_id=dag_id,
            task_id=task_id,
            ts=ts,
            log_url=log_url,
            s3_path=s3_path,
        )

        smtp = SmtpHook(smtp_conn_id=SMTP_CONN_ID)
        smtp.send_email_smtp(
            to=DEFAULT_TO,
            subject=subject,
            html_content=body.replace("\n", "<br>"),  # simples (texto → html)
            from_email=DEFAULT_FROM,
        )

        logging.info("Email enviado para %s", DEFAULT_TO)

    # Sequência solicitada
    local_path = extrair_dados_oracle()
    s3_path = enviar_para_minio(local_path)
    email = enviar_email(s3_path)

    start >> local_path >> s3_path >> email >> end


pipeline_pda_minio()
