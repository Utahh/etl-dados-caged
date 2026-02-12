import sys
import os
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

# --- FIX DE IMPORTAÇÃO (ADICIONE ISTO) ---
# Adiciona a raiz do projeto (/opt/airflow) ao caminho do Python
# Isso permite que 'from src.pipeline' funcione corretamente
PROJECT_ROOT = "/opt/airflow"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# --- SETUP DE CAMINHOS ---
dag_folder = os.path.dirname(os.path.abspath(__file__))
# sys.path.append(dag_folder) # Não é estritamente necessário se usar o fix acima
airflow_home = os.path.dirname(dag_folder)
# sys.path.append(airflow_home) # Redundante com o fix acima, mas pode manter se quiser

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
        # Importação dentro da função para garantir que o sys.path já foi atualizado
        try:
            from src.pipeline import run_pipeline
        except ImportError as e:
            logging.error(f"⚠️ Erro de importação: {e}")
            # Tenta debug do caminho
            logging.error(f"🔍 sys.path atual: {sys.path}")
            logging.error(f"🔍 Conteúdo de /opt/airflow: {os.listdir('/opt/airflow')}")
            raise e

        # Captura parâmetros
        params = kwargs['params']
        modo = params.get('modo_execucao', 'Automatico')
        
        logging.info(f"⚙️ MODO SELECIONADO: {modo}")
        
        lista_processamento = []

        # 1. AUTOMÁTICO (Janela Deslizante de 12 Meses)
        # Atenção: Ajustei a lógica do automático pois seu código original tinha um pequeno bug no loop
        # que calculava datas futuras ou incorretas. Esta versão está segura.
        if modo == "Automatico":
            logging.info("📅 Calculando janela móvel de 12 meses...")
            
            hoje = datetime.now()
            # Lag de 2 meses para trás (ex: Em Março, processa Janeiro para trás)
            # data_base = data de referência inicial (ex: 2025-01-01)
            # Se hoje é Março (3), data_base vira Janeiro (1)
            # Se hoje é Janeiro (1), data_base vira Novembro do ano passado
            
            # Lógica Simplificada de Data Retroativa:
            # Vamos gerar as datas voltando mês a mês a partir de hoje - 2 meses
            meses_atras = 2
            
            for i in range(12): # 12 meses de histórico
                # Calcula a data alvo subtraindo meses
                # Formula: Meses totais = (Ano * 12 + Mes - 1) - (lag + i)
                total_meses = (hoje.year * 12 + hoje.month - 1) - (meses_atras + i)
                ano_calc = total_meses // 12
                mes_calc = (total_meses % 12) + 1
                
                lista_processamento.append((str(ano_calc), str(mes_calc).zfill(2)))
            
            logging.info(f"🤖 Automático detectou janela: {lista_processamento}")

        # 2. MANUAL (MÊS ÚNICO OU PERÍODO)
        else:
            ano_i = params.get('ano_inicio')
            mes_i = params.get('mes_inicio')
            
            if not ano_i or not mes_i:
                raise ValueError("⚠️ Ano e Mês INICIAIS são obrigatórios para modo Manual!")

            # MODO MÊS ÚNICO
            if modo == "Manual_Mes_Unico":
                lista_processamento = [(ano_i, mes_i.zfill(2))]
            
            # MODO PERÍODO
            elif modo == "Manual_Periodo":
                ano_f = params.get('ano_fim')
                mes_f = params.get('mes_fim')
                
                if not ano_f or not mes_f:
                    raise ValueError("⚠️ Para período, Ano e Mês FINAIS são obrigatórios!")

                dt_atual = datetime(int(ano_i), int(mes_i), 1)
                dt_fim = datetime(int(ano_f), int(mes_f), 1)

                if dt_fim < dt_atual:
                    raise ValueError("⚠️ A data final deve ser maior ou igual à inicial.")

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

        logging.info(f"📋 Processando {len(lista_processamento)} competências...")

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