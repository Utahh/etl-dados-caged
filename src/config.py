import os

# --- 1. Configuração de Caminhos ---
IS_DOCKER = os.path.exists('/.dockerenv') or os.getenv('AIRFLOW_HOME') is not None
BASE_PATH = '/opt/airflow' if IS_DOCKER else os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_DIR = os.path.join(BASE_PATH, 'data')
RAW_DIR = os.path.join(DATA_DIR, 'raw')
RAW_ZIP_DIR = os.path.join(RAW_DIR, 'zip')
RAW_EXTRACT_DIR = os.path.join(RAW_DIR, 'extracted')
PROCESSED_DIR = os.path.join(DATA_DIR, 'processed')
REFS_DIR = os.path.join(DATA_DIR, 'refs')

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

# MAPA BLINDADO: Inclui nomes originais, técnicos e versões CORROMPIDAS comuns
COLUMNS_MAP = {
    # --- MUNICÍPIO ---
    'município': 'municipio_codigo',
    'municipio_codigo': 'municipio_codigo',
    'municapio': 'municipio_codigo',   # <--- Do seu log
    'municipio': 'municipio_codigo',

    # --- ATIVIDADE (CNAE/SEÇÃO) ---
    'subclasse': 'subclasse_codigo',
    'subclasse_codigo': 'subclasse_codigo',
    'seção': 'secao_codigo',
    'secao_codigo': 'secao_codigo',
    'seaao': 'secao_codigo',           # <--- Do seu log
    'secao': 'secao_codigo',

    # --- MOVIMENTAÇÃO E SALDO ---
    'competênciamov': 'competencia_mov',
    'competencia_mov': 'competencia_mov',
    'competaanciamov': 'competencia_mov', # <--- Do seu log
    
    'saldo': 'saldo_movimentacao',
    'saldo_movimentacao': 'saldo_movimentacao',
    'saldomovimentaaao': 'saldo_movimentacao', # <--- Do seu log
    
    'tipomovimentação': 'tipo_movimentacao',
    'tipo_movimentacao_codigo': 'tipo_movimentacao',
    'tipomovimentaaao': 'tipo_movimentacao',   # <--- Do seu log

    # --- SALÁRIO ---
    'salário': 'salario',
    'salario': 'salario',
    'valorsaláriofixo': 'salario',
    'valorsalariofixo': 'salario',     # <--- Do seu log

    # --- DADOS DEMOGRÁFICOS ---
    'raçacor': 'raca_cor',
    'raca_cor_codigo': 'raca_cor',
    'raaacor': 'raca_cor',             # <--- Do seu log
    
    'sexo': 'sexo',
    'sexo_codigo': 'sexo',
    
    'idade': 'idade',
    
    'graudeinstrução': 'grau_instrucao',
    'grau_instrucao_codigo': 'grau_instrucao',
    'graudeinstruaao': 'grau_instrucao', # <--- Do seu log

    # --- OUTROS ---
    'cbo2002ocupação': 'cbo2002',
    'cbo2002ocupaaao': 'cbo2002',       # <--- Do seu log
    'categoria': 'categoria',
    'categoria_codigo': 'categoria',
    'horascontratuais': 'horas_contratuais',
    'tipoempregador': 'tipo_empregador',
    'tipoestabelecimento': 'tipo_estabelecimento',
    'indtrabalhointermitente': 'ind_trabalho_intermitente',
    'indtrabintermitente': 'ind_trabalho_intermitente', # <--- Do seu log
    'indtrabalhoparcial': 'ind_trabalho_parcial',
    'indtrabparcial': 'ind_trabalho_parcial'            # <--- Do seu log
}