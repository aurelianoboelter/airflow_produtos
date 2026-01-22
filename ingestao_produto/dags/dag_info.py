import subprocess
import sys

from airflow.sdk import dag, task


def get_python_version():
    print("--- Versão do Python ---")
    print(sys.version)


def get_pip_freeze():
    print("--- Lista de Pacotes (pip freeze) ---")
    # Executa o comando pip freeze no shell do worker e captura a saída
    result = subprocess.run(
        [sys.executable, "-m", "pip", "freeze"], capture_output=True, text=True
    )
    print(result.stdout)


@dag(dag_id="dag_info")
def dag_info():

    @task
    def get_python_version():
        print("--- Versão do Python ---")
        print(sys.version)

    @task
    def get_pip_freeze():
        print("--- Lista de Pacotes (pip freeze) ---")
        # Executa o comando pip freeze no shell do worker e captura a saída
        result = subprocess.run(
            [sys.executable, "-m", "pip", "freeze"], capture_output=True, text=True
        )
        print(result.stdout)

    task_python_version = get_python_version()

    task_pip_freeze = get_pip_freeze()

    task_python_version >> task_pip_freeze


dag_info()
