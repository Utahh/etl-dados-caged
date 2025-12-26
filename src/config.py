import os

# --- 1. Configuração de Caminhos ---
IS_DOCKER = os.path.exists('/.dockerenv') or os.getenv('AIRFLOW_HOME') is not None

if IS_DOCKER:
    BASE_PATH = '/opt/airflow'
else:
    BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_DIR = os.path.join(BASE_PATH, 'data')
RAW_DIR = os.path.join(DATA_DIR, 'raw')
RAW_ZIP_DIR = os.path.join(RAW_DIR, 'zip')
RAW_EXTRACT_DIR = os.path.join(RAW_DIR, 'extracted')
PROCESSED_DIR = os.path.join(DATA_DIR, 'processed')
REFS_DIR = os.path.join(DATA_DIR, 'refs')

# Garante que as pastas existam
os.makedirs(RAW_ZIP_DIR, exist_ok=True)
os.makedirs(RAW_EXTRACT_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

# --- 2. Configurações do FTP ---
FTP_HOST = 'ftp.mtps.gov.br'
FTP_BASE_PATH = '/pdet/microdados/NOVO CAGED/'
MAX_HISTORY_FILES = 3

# --- 3. Configuração do Banco de Dados ---
DB_URL = os.getenv('DB_URL', 'postgresql+psycopg2://airflow:airflow@postgres/airflow')
TABLE_NAME = 'caged_sp_novo'

# --- 4. Filtros e Mapeamentos ---
UF_FILTER = 35  # São Paulo

# Mapa de colunas (Nome no Arquivo -> Nome no Banco)
COLUMNS_MAP = {
    'competênciamov': 'competencia_mov',
    'município': 'municipio_codigo',
    'subclasse': 'subclasse_codigo',
    'seção': 'secao_codigo',
    'cbo2002ocupação': 'cbo2002',
    'categoria': 'categoria',
    'graudeinstrução': 'grau_instrucao',
    'idade': 'idade',
    'horascontratuais': 'horas_contratuais',
    'raçacor': 'raca_cor',
    'sexo': 'sexo',
    'tipoempregador': 'tipo_empregador',
    'tipoestabelecimento': 'tipo_estabelecimento',
    'tipomovimentação': 'tipo_movimentacao',
    'indtrabalhointermitente': 'ind_trabalho_intermitente',
    'indtrabalhoparcial': 'ind_trabalho_parcial',
    # 'salário': 'salario', <-- REMOVIDO para evitar duplicidade
    'valorsaláriofixo': 'salario', # Prioridade
    'saldo': 'saldo_movimentacao'
}