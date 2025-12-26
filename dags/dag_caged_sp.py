from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
import sys

hoje = datetime.now()
mes_passado = hoje.replace(day=1) - timedelta(days=1)
default_ano = str(mes_passado.year)
default_mes = str(mes_passado.month).zfill(2)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

with DAG(
    'dag_caged_sp_manual_v2',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params={
        "ano": Param(default_ano, type="string", description="Ano (YYYY)"),
        "mes": Param(default_mes, type="string", description="Mês (MM)"),
    }
) as dag:

    def task_wrapper(**kwargs):
        if '/opt/airflow' not in sys.path:
            sys.path.insert(0, '/opt/airflow')
            
        try:
            from src.pipeline import run_pipeline
        except ImportError as e:
            print(f"❌ Erro ao importar pipeline: {e}")
            raise e

        params = kwargs['params']
        print(f"🚀 Iniciando Pipeline Manual para: {params['mes']}/{params['ano']}")
        run_pipeline(year=params['ano'], month=params['mes'])

    run_task = PythonOperator(
        task_id='run_caged_pipeline',
        python_callable=task_wrapper
    )