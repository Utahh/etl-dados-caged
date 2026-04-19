import sys
import logging
from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

# --- FIX DE IMPORTAÇÃO ---
PROJECT_ROOT = "/opt/airflow"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# Fuso horário de Brasília/SP para evitar bugs na janela móvel Automática
local_tz = pendulum.timezone("America/Sao_Paulo")

# --- CONFIGURAÇÃO DA DAG ---
default_args_config = { 
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1, # Retry de segurança caso o FTP do governo oscile
}

params_config = {
    "modo_execucao": Param("Manual_Periodo", enum=["Automatico", "Manual_Mes_Unico", "Manual_Periodo"], description="Modo de Execução"),
    "ano_inicio": Param("2025", type=["string", "null"], description="[Manual] Ano Início (Ex: 2025)"),
    "mes_inicio": Param("01", type=["string", "null"], description="[Manual] Mês Início (Ex: 01)"),
    "ano_fim": Param("2025", type=["string", "null"], description="[Periodo] Ano Fim (Ex: 2025)"),
    "mes_fim": Param("12", type=["string", "null"], description="[Periodo] Mês Fim (Ex: 12)"),
}

with DAG(
    'dag_caged_sp_v5_final',
    default_args=default_args_config,
    description='Pipeline CAGED SP - Carga Otimizada Lakehouse',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=['caged', 'botucatu', 'producao', 'addai-core'],
    params=params_config
) as dag:

    def task_wrapper(**kwargs):
        try:
            # --- O AJUSTE CRÍTICO DA ARQUITETURA LAKEHOUSE ESTÁ AQUI ---
            from src.domains.caged.pipeline import run_pipeline, refresh_sql_model
        except ImportError as e:
            logging.error(f"⚠️ Erro de importação: {e}")
            logging.error(f"🔍 sys.path atual: {sys.path}")
            raise e

        # --- 1. CAPTURA PARÂMETROS ---
        params = kwargs['params']
        modo = params.get('modo_execucao', 'Manual_Periodo')
        
        logging.info(f"⚙️ MODO SELECIONADO: {modo}")
        lista_processamento = []

        # Lógica Automática com Fuso Horário Correto
        if modo == "Automatico":
            logging.info("📅 Calculando janela móvel de 12 meses...")
            hoje = pendulum.now(local_tz)
            meses_atras = 2 
            
            for i in range(12):
                total_meses = (hoje.year * 12 + hoje.month - 1) - (meses_atras + i)
                ano_calc = total_meses // 12
                mes_calc = (total_meses % 12) + 1
                lista_processamento.append((str(ano_calc), str(mes_calc).zfill(2)))

        # Lógica Manual
        else:
            ano_i = params.get('ano_inicio')
            mes_i = params.get('mes_inicio')
            
            if not ano_i or not mes_i:
                raise ValueError("⚠️ Ano e Mês INICIAIS são obrigatórios!")

            if modo == "Manual_Mes_Unico":
                lista_processamento = [(ano_i, str(mes_i).zfill(2))]
            
            elif modo == "Manual_Periodo":
                ano_f = params.get('ano_fim')
                mes_f = params.get('mes_fim')
                
                if not ano_f or not mes_f:
                    raise ValueError("⚠️ Ano e Mês FINAIS são obrigatórios!")

                dt_atual = datetime(int(ano_i), int(mes_i), 1)
                dt_fim = datetime(int(ano_f), int(mes_f), 1)

                if dt_fim < dt_atual:
                    raise ValueError("⚠️ A data final deve ser maior ou igual à inicial.")

                while dt_atual <= dt_fim:
                    lista_processamento.append((str(dt_atual.year), str(dt_atual.month).zfill(2)))
                    if dt_atual.month == 12:
                        dt_atual = dt_atual.replace(year=dt_atual.year + 1, month=1)
                    else:
                        dt_atual = dt_atual.replace(month=dt_atual.month + 1)

        # --- 2. EXECUÇÃO DA CARGA (LOOP) ---
        if not lista_processamento:
            logging.warning("⚠️ Nenhuma data para processar.")
            return

        # Ordena a lista do mais antigo para o mais novo cronologicamente
        lista_processamento.sort(key=lambda x: (x[0], x[1]))

        logging.info(f"📋 Processando {len(lista_processamento)} competências...")

        for idx, (ano, mes) in enumerate(lista_processamento):
            logging.info(f"▶️ [{idx+1}/{len(lista_processamento)}] Carga de: {mes}/{ano}...")
            # Como a nossa carga usa DELETE antes do COPY, é 100% segura para reprocessar
            run_pipeline(ano, mes) 

        # --- 3. OTIMIZAÇÃO FINAL ---
        logging.info("\n---")
        logging.info("🏁 Carga de dados finalizada com sucesso.")
        logging.info("🏗️ Rodando Otimização do Modelo (Star Schema e Índices)...")
        
        refresh_sql_model() 
        
        logging.info("🚀 Pipeline concluído! O Power BI já pode ser atualizado.")

    run_task = PythonOperator(
        task_id='run_caged_pipeline_completo',
        python_callable=task_wrapper
    )