import csv
import logging
import os

import pendulum
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.smtp.notifications.smtp import send_smtp_notification

# =============================================================================
# Bloco 1: Configurações Gerais
# =============================================================================
ORACLE_CONN_ID = "oracle_trans_ingestao_produto"
MINIO_CONN_ID = "minio_conn"
MINIO_BUCKET = "bi-pda"
MINIO_EXPORT_PATH = "exports/teste_pda"
CSV_ENCODING = "cp1252"
CSV_SEPARATOR = ";"

# Diretório temporário para salvar o arquivo entre as tasks
TEMP_DATA_DIR = "/tmp"

# =============================================================================
# Bloco 2: Configurações de Notificação
# =============================================================================
DEFAULT_FROM = "airflow@ans.gov.br"
DEFAULT_TO = [
    "aureliano.silva@ans.gov.br",
    "thiago.capello@ans.gov.br",
    "alexandre.santos@ans.gov.br",
    "fabricio.francisco@ans.gov.br",
]

LOG_URL_REPLACE = {
    "old": "http://localhost:8080",
    "new": "http://airflow-ds.ans.gov.br:8080",
}

# --- TEMPLATE DE FALHA ---
FAILURE_SUBJECT = (
    "[Airflow - FALHA] Exportação PDA Minio falhou (Task: {{ ti.task_id }})"
)
FAILURE_HTML = f"""
<html>
  <body style="font-family: Arial, sans-serif; color:#333; background-color:#f8f9fa; padding: 20px;">
    <table width="100%%" style="max-width: 600px; margin:auto; background-color:#fff; border-radius: 8px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); padding: 20px;">
      <tr>
        <td>
          <h2 style="color:#d9534f; text-align:center;"> Alerta de Falha - Processo Airflow</h2>
          <p>Olá,</p>
          <p>A tarefa <strong>{{{{ ti.task_id }}}}</strong> da DAG <strong>{{{{ dag.dag_id }}}}</strong> falhou.</p>

          <p><b>DAG:</b> {{{{ dag.dag_id }}}}<br>
             <b>Tarefa:</b> {{{{ ti.task_id }}}}<br>
             <b>Execução:</b> {{{{ ts }}}}</p>

          <p>Acesse os logs para mais detalhes:</p>
          <p style="text-align:center; margin:20px 0;">
            <a href="{{{{ ti.log_url | replace('{LOG_URL_REPLACE['old']}', '{LOG_URL_REPLACE['new']}') }}}}"
               style="background-color:#d9534f; color:white; text-decoration:none; padding:10px 20px; border-radius:5px;">
              🔗 Abrir Logs de Erro
            </a>
          </p>
          <hr style="border:none; border-top:1px solid #ddd; margin:20px 0;">
          <p style="font-size: 0.85em; color:#666; text-align:center;">
            Mensagem automática - Sistema Airflow ANS
          </p>
        </td>
      </tr>
    </table>
  </body>
</html>
"""

# --- TEMPLATE DE SUCESSO ---
SUCCESS_SUBJECT = "[Airflow - SUCESSO] Exportação PDA Minio concluída!"
SUCCESS_HTML = f"""
<html>
  <body style="font-family: Arial, sans-serif; color:#333; background-color:#f8f9fa; padding: 20px;">
    <table width="100%%" style="max-width: 600px; margin:auto; background-color:#fff; border-radius: 8px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); padding: 20px;">
      <tr>
        <td>
          <h2 style="color:#28a745; text-align:center;"> Sucesso - Processo Airflow</h2>
          <p>Olá,</p>
          <p>A DAG <strong>{{{{ dag.dag_id }}}}</strong> foi executada com sucesso.</p>

          <p><b>DAG:</b> {{{{ dag.dag_id }}}}<br>
             <b>Tarefa:</b> {{{{ ti.task_id }}}}<br>
             <b>Execução:</b> {{{{ ts }}}}</p>

          <p style="background-color:#f0f0f0; padding: 12px; border-radius: 4px; border-left: 5px solid #28a745;">
             <b>Arquivo Gerado:</b><br>
             {{{{ ti.xcom_pull(task_ids='enviar_para_minio') }}}}
          </p>

          <p>Para mais detalhes, acesse os logs da execução:</p>
          <p style="text-align:center; margin:20px 0;">
            <a href="{{{{ ti.log_url | replace('{LOG_URL_REPLACE['old']}', '{LOG_URL_REPLACE['new']}') }}}}"
               style="background-color:#007bff; color:white; text-decoration:none; padding:10px 20px; border-radius:5px;">
              🔗 Abrir Logs no Airflow
            </a>
          </p>
          <hr style="border:none; border-top:1px solid #ddd; margin:20px 0;">
          <p style="font-size: 0.85em; color:#666; text-align:center;">
            Mensagem automática - Sistema Airflow ANS
          </p>
        </td>
      </tr>
    </table>
  </body>
</html>
"""

# Configuração dos Callbacks
notify_failure = send_smtp_notification(
    from_email=DEFAULT_FROM,
    to=DEFAULT_TO,
    subject=FAILURE_SUBJECT,
    html_content=FAILURE_HTML,
)

notify_success = send_smtp_notification(
    from_email=DEFAULT_FROM,
    to=DEFAULT_TO,
    subject=SUCCESS_SUBJECT,
    html_content=SUCCESS_HTML,
)

# =============================================================================
# Bloco 3: Definição da DAG com TaskFlow API
# =============================================================================


@dag(
    dag_id="dag_exporta_pda_minio_teste_meu",
    start_date=pendulum.datetime(2025, 8, 29, tz="America/Sao_Paulo"),
    schedule=None,
    catchup=False,
    tags=["pda", "minio", "export", "notificação"],
    # Callback padrão para TODAS as tasks da DAG (se extração falhar, avisa)
    on_failure_callback=[notify_failure],
)
def pipeline_pda_minio():

    # --- Task 1: Consulta Oracle e salva localmente ---
    @task(task_id="extrair_dados_oracle")
    def extrair_dados_oracle(**context):
        ts_nodash = context["ts_nodash"]
        file_name = f"extracao_pda_{ts_nodash}.csv"
        local_file_path = os.path.join(TEMP_DATA_DIR, file_name)

        logging.info(f"Iniciando consulta Oracle. Destino local: {local_file_path}")

        oracle_hook = OracleHook(oracle_conn_id=ORACLE_CONN_ID)

        with oracle_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT
                           TBREGS.ID_PLANO,
                           TAS.CD_SERVICO_OPCIONAL,
                           TAS.DE_SERVICO_OPCIONAL AS SERVICO_OPCIONAL
                    FROM DIPRO.TB_REGS_PLANO TBREGS
                         INNER JOIN DIPRO.RL_REGS_PLANO_SERVICO_OPCIONAL REGPL
                            ON TBREGS.ID_PLANO = REGPL.ID_PLANO
                         INNER JOIN DIPRO.TA_SERVICO_OPCIONAL TAS
                            ON REGPL.CD_SERVICO_OPCIONAL = TAS.CD_SERVICO_OPCIONAL
                """
                )

                headers = [desc[0].lower() for desc in cur.description]

                # Escrevendo direto no disco para economizar memória
                with open(local_file_path, "w", newline="", encoding=CSV_ENCODING) as f:
                    writer = csv.writer(
                        f,
                        delimiter=CSV_SEPARATOR,
                        quotechar='"',
                        quoting=csv.QUOTE_MINIMAL,
                    )
                    writer.writerow(headers)

                    total_rows = 0
                    chunk_size = 5000
                    while True:
                        rows = cur.fetchmany(chunk_size)
                        if not rows:
                            break
                        writer.writerows(rows)
                        total_rows += len(rows)

        logging.info(
            f"Extração concluída: {total_rows} linhas salvas em {local_file_path}"
        )
        return local_file_path

    # --- Task 2: Pega arquivo local e envia para MinIO ---
    @task(
        task_id="enviar_para_minio",
        # Apenas enviamos email de SUCESSO se esta task terminar
        on_success_callback=[notify_success],
    )
    def enviar_para_minio(local_file_path: str):
        # Extrai apenas o nome do arquivo do caminho completo
        file_name = os.path.basename(local_file_path)
        minio_key = f"{MINIO_EXPORT_PATH}/{file_name}"

        logging.info(f"Lendo arquivo local: {local_file_path}")
        logging.info(f"Enviando para MinIO: {MINIO_BUCKET}/{minio_key}")

        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

        s3_hook.load_file(
            filename=local_file_path,
            key=minio_key,
            bucket_name=MINIO_BUCKET,
            replace=True,
        )

        # Limpeza opcional do arquivo local após upload
        try:
            os.remove(local_file_path)
            logging.info("Arquivo local temporário removido.")
        except Exception as e:
            logging.warning(f"Não foi possível remover arquivo local: {e}")

        full_s3_path = f"s3://{MINIO_BUCKET}/{minio_key}"
        logging.info(f"Upload finalizado: {full_s3_path}")

        return full_s3_path

    # --- Definindo o Fluxo ---
    # A saída da Task 1 (path) entra como argumento na Task 2
    arquivo_gerado = extrair_dados_oracle()
    enviar_para_minio(arquivo_gerado)


# Instancia a DAG
pipeline_pda_minio()
