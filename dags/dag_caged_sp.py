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
    "mes_inicio": Param("", type=["string", "null"], description="[Manual] Mês (Ex: 04)"),
    "ano_fim": Param("", type=["string", "null"], description="[Periodo] Ano Final (Ex: 2025)"),
    "mes_fim": Param("", type=["string", "null"], description="[Periodo] Mês Final (Ex: 08)"),
}

with DAG(
    'dag_caged_sp_manual_v2',
    default_args=default_args_config,
    description='Pipeline CAGED SP (Automático 12 meses e Manual)',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['caged', 'sp', 'etl'],
    params=params
) as dag:

    def task_wrapper(**kwargs):
        try:
            from src.pipeline import run_pipeline
        except ImportError as e:
            logging.error(f"⚠️ Erro de importação: {e}")
            raise e

        # Captura parâmetros
        params = kwargs['params']
        modo = params.get('modo_execucao', 'Automatico')
        
        logging.info(f"⚙️ MODO SELECIONADO: {modo}")
        
        lista_processamento = []

        # 1. AUTOMÁTICO (Janela Deslizante de 12 Meses)
        if modo == "Automatico":
            logging.info("📅 Calculando janela móvel de 12 meses (Regra do CAGED Vivo)...")
            
            hoje = datetime.now()
            data_base = hoje.replace(day=1)
            
            # Lag: Começamos 2 meses atrás (pois o governo demora ~45 dias para liberar o último)
            # Duração: Voltamos 12 meses a partir daí para atualizar o histórico
            lag_meses = 2 
            janela_retroativa = 12 
            
            for i in range(lag_meses, lag_meses + janela_retroativa): 
                # Matemática segura para voltar meses
                mes_calc = (data_base.month - i - 1) % 12 + 1
                ano_calc = data_base.year + (data_base.month - i - 1) // 12
                
                lista_processamento.append((str(ano_calc), str(mes_calc).zfill(2)))
            
            logging.info(f"🤖 Automático detectou janela de atualização: {lista_processamento}")

        # 2. MANUAL (MÊS ÚNICO OU PERÍODO)
        else:
            ano_i = params.get('ano_inicio')
            mes_i = params.get('mes_inicio')
            
            if not ano_i or not mes_i:
                raise ValueError("⚠️ Ano e Mês INICIAIS são obrigatórios!")

            # MODO MÊS ÚNICO
            if modo == "Manual_Mes_Unico":
                lista_processamento = [(ano_i, mes_i.zfill(2))]
            
            # MODO PERÍODO
            elif modo == "Manual_Periodo":
                ano_f = params.get('ano_fim')
                mes_f = params.get('mes_fim')
                
                if not ano_f or not mes_f:
                    raise ValueError("⚠️ Para período, Ano e Mês FINAIS são obrigatórios!")

                # Converte para data para poder fazer o loop
                dt_atual = datetime(int(ano_i), int(mes_i), 1)
                dt_fim = datetime(int(ano_f), int(mes_f), 1)

                if dt_fim < dt_atual:
                    raise ValueError("⚠️ A data final deve ser maior ou igual à inicial.")

                # Loop mês a mês
                while dt_atual <= dt_fim:
                    lista_processamento.append((str(dt_atual.year), str(dt_atual.month).zfill(2)))
                    
                    # Avança 1 mês
                    if dt_atual.month == 12:
                        dt_atual = dt_atual.replace(year=dt_atual.year + 1, month=1)
                    else:
                        dt_atual = dt_atual.replace(month=dt_atual.month + 1)

        # --- EXECUÇÃO ---
        if not lista_processamento:
            logging.warning("⚠️ Nenhuma data para processar.")
            return

        logging.info(f"📋 Processando {len(lista_processamento)} competências: {lista_processamento}")

        for idx, (ano, mes) in enumerate(lista_processamento):
            logging.info(f"\n▶️ [{idx+1}/{len(lista_processamento)}] Iniciando carga de: {mes}/{ano}...")
            try:
                run_pipeline(ano, mes)
            except Exception as e:
                logging.error(f"❌ Falha crítica em {mes}/{ano}: {e}")
                raise e 

    run_task = PythonOperator(
        task_id='run_caged_pipeline_v2',
        python_callable=task_wrapper,
        provide_context=True
    )