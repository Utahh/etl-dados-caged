import sys
import os
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

# --- SETUP DE CAMINHOS ---
dag_folder = os.path.dirname(os.path.abspath(__file__))
sys.path.append(dag_folder)
airflow_home = os.path.dirname(dag_folder)
sys.path.append(airflow_home)

# --- CONFIGURAÇÃO DA DAG ---
default_args_config = { 
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# Parâmetros
params = {
    "modo_execucao": Param("Automatico", enum=["Automatico", "Manual_Mes_Unico", "Manual_Periodo"], description="Modo de Execução"),
    "ano_inicio": Param("", type=["string", "null"], description="[Manual] Ano (Ex: 2025)"),
    "mes_inicio": Param("", type=["string", "null"], description="[Manual] Mês (Ex: 11)"),
    "ano_fim": Param("", type=["string", "null"], description="[Periodo] Ano Final"),
    "mes_fim": Param("", type=["string", "null"], description="[Periodo] Mês Final"),
}

with DAG(
    'dag_caged_sp_manual_v2',
    default_args=default_args_config, # <--- Está correto aqui. Pode ignorar o sublinhado do editor.
    description='Pipeline CAGED SP (Automático 3 meses e Manual)',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['caged', 'sp', 'etl'],
    params=params
) as dag:

    def task_wrapper(**kwargs):
        # Importação segura
        try:
            from src.pipeline import run_pipeline
            from src.ftp_client import FTPClient
            from src.config import FTP_HOST
        except ImportError as e:
            logging.error(f"⚠️ Erro de importação: {e}")
            raise e

        # Captura parâmetros
        params = kwargs['params']
        modo = params.get('modo_execucao', 'Automatico')
        
        logging.info(f"⚙️ MODO SELECIONADO: {modo}")
        
        lista_processamento = []

        if modo == "Automatico":
            logging.info("📅 Calculando os últimos 3 meses disponíveis...")
            hoje = datetime.now()
            data_base = hoje.replace(day=1)
            
            for i in range(2, 5): 
                mes_anterior = (data_base.month - i - 1) % 12 + 1
                ano_anterior = data_base.year + (data_base.month - i - 1) // 12
                lista_processamento.append((str(ano_anterior), str(mes_anterior).zfill(2)))
            
            logging.info(f"🤖 Automático detectou: {lista_processamento}")

        else:
            ano_i = params.get('ano_inicio')
            mes_i = params.get('mes_inicio')
            
            if not ano_i or not mes_i:
                raise ValueError("⚠️ Para modo Manual, Ano e Mês de início são obrigatórios!")

            if modo == "Manual_Mes_Unico":
                lista_processamento = [(ano_i, mes_i.zfill(2))]
            elif modo == "Manual_Periodo":
                lista_processamento = [(ano_i, mes_i.zfill(2))]

        if not lista_processamento:
            logging.warning("⚠️ Nenhuma data para processar.")
            return

        for idx, (ano, mes) in enumerate(lista_processamento):
            logging.info(f"\n▶️ [{idx+1}/{len(lista_processamento)}] Iniciando carga de: {mes}/{ano}...")
            run_pipeline(ano, mes)

    run_task = PythonOperator(
        task_id='run_caged_pipeline_v2',
        python_callable=task_wrapper,
        provide_context=True
    )