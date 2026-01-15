from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
import sys
import gc   # <--- Importante para limpeza de memória
import time # <--- Importante para o "respiro"

# --- Configurações Padrão ---
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
    tags=['caged', 'sp', 'etl', 'otimizado'],
    params={
        "modo_execucao": Param(
            "Automático (Disponível no Governo)", 
            type="string", 
            enum=["Automático (Disponível no Governo)", "Manual (Mês Único)", "Manual (Período)"],
            description="Automático: Varre o FTP e baixa os 3 últimos meses reais disponíveis."
        ),
        "ano_inicio": Param(default_ano, type=["string", "null"], description="Ano Início (Ignorar se Automático)"),
        "mes_inicio": Param(default_mes, type=["string", "null"], description="Mês Início (Ignorar se Automático)"),
        "ano_fim": Param(None, type=["string", "null"], description="Ano Fim (Apenas para Modo Período)"),
        "mes_fim": Param(None, type=["string", "null"], description="Mês Fim (Apenas para Modo Período)"),
    }
) as dag:

    def task_wrapper(**kwargs):
        if '/opt/airflow' not in sys.path:
            sys.path.insert(0, '/opt/airflow')
            
        try:
            from src.pipeline import run_pipeline
            from src.ftp_client import FTPClient
        except ImportError as e:
            print(f"❌ Erro crítico de importação: {e}")
            raise e

        # --- SETUP ---
        params = kwargs['params']
        modo = params['modo_execucao']
        lista_processamento = []

        print(f"⚙️ MODO SELECIONADO: {modo}")

        # --- DEFINIÇÃO DA LISTA DE TRABALHO ---
        if modo == "Automático (Disponível no Governo)":
            print("📡 Conectando ao FTP (Modo Seguro Latin-1)...")
            try:
                ftp = FTPClient()
                lista_processamento = ftp.get_available_periods(limit=3)
                if not lista_processamento:
                    print("⚠️ Aviso: Nenhum período encontrado.")
                else:
                    lista_processamento.sort() # Ordena cronologicamente
            except Exception as e:
                print(f"❌ Erro FTP: {e}")
                raise e

        elif modo == "Manual (Período)":
            if not params['ano_fim'] or not params['mes_fim']:
                raise ValueError("Preencha Ano/Mês Fim para período.")
            try:
                dt_inicio = datetime(int(params['ano_inicio']), int(params['mes_inicio']), 1)
                dt_fim = datetime(int(params['ano_fim']), int(params['mes_fim']), 1)
                if dt_fim < dt_inicio: raise ValueError("Data Fim menor que Início.")
                curr = dt_inicio
                while curr <= dt_fim:
                    lista_processamento.append((str(curr.year), str(curr.month).zfill(2)))
                    curr += relativedelta(months=1)
            except Exception as e:
                print(f"❌ Erro datas: {e}")
                raise e

        else: # Mês Único
            lista_processamento.append((params['ano_inicio'], params['mes_inicio']))

        # ======================================================================
        # LOOP DE EXECUÇÃO OTIMIZADO (BATCH SAFE)
        # ======================================================================
        total = len(lista_processamento)
        print(f"📋 Fila de Processamento: {lista_processamento}")
        
        for idx, (ano, mes) in enumerate(lista_processamento):
            print(f"\n▶️ [{idx+1}/{total}] Iniciando carga de: {mes}/{ano}...")
            
            start_time = time.time()
            try:
                # 1. Executa o Pipeline Pesado
                run_pipeline(year=ano, month=mes)
                
            except Exception as e:
                print(f"⚠️ Falha ao processar {mes}/{ano}: {e}")
                # Continua para o próximo mesmo com erro
            
            finally:
                # 2. ESTRATÉGIA ANTI-SOBRECARGA
                duration = time.time() - start_time
                print(f"⏱️ Duração da etapa: {duration:.2f} segundos.")
                
                print("🧹 Faxina: Liberando memória RAM (Garbage Collection)...")
                gc.collect()  # Força limpeza da RAM
                
                print("💤 Respiro: Aguardando 5s para aliviar FTP e Banco...")
                time.sleep(5) # Pausa para evitar rate-limit e baixar uso de CPU

        print(f"\n🏁 Execução finalizada com sucesso.")

    run_task = PythonOperator(
        task_id='run_caged_pipeline_v2',
        python_callable=task_wrapper
    )