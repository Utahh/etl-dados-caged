import os

# --- DIRETÓRIOS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.basename(BASE_DIR) == 'src':
    BASE_DIR = os.path.dirname(BASE_DIR)

DATA_DIR = os.path.join(BASE_DIR, 'data')
RAW_ZIP_DIR = os.path.join(DATA_DIR, 'raw', 'zip')
RAW_EXTRACT_DIR = os.path.join(DATA_DIR, 'raw', 'extracted')
PROCESSED_DIR = os.path.join(DATA_DIR, 'processed')
REFS_DIR = os.path.join(DATA_DIR, 'refs')

# --- CONFIGURAÇÕES FTP ---
FTP_HOST = "ftp.mtps.gov.br"
FTP_BASE_PATH = "/pdet/microdados/NOVO CAGED/"
MAX_HISTORY_FILES = 3

# --- FILTROS ---
UF_FILTER = 35  # São Paulo (None para processar tudo)

# --- BANCO DE DADOS ---
DB_HOST = "postgres" 
DB_PORT = "5432"
DB_NAME = "airflow"
DB_USER = "airflow"
DB_PASS = "airflow"
TABLE_NAME = "caged_sp_novo"
DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- MAPEAMENTO DE COLUNAS ---
# A Lógica é: 'nome_no_arquivo_txt': 'nome_no_banco_de_dados'
COLUMNS_MAP = {
    # MUNICÍPIO
    'município': 'municipio_codigo',
    'municipio': 'municipio_codigo',
    
    # SUBCLASSE (Essencial para o Join)
    'subclasse': 'subclasse_codigo',
    
    # MOVIMENTAÇÃO
    'competênciamov': 'competencia_mov',
    'competencia': 'competencia_mov',
    'saldo': 'saldo_movimentacao',
    'saldomovimentação': 'saldo_movimentacao',
    'tipomovimentação': 'tipo_movimentacao',
    
    # SALÁRIO (Aqui estava o problema, simplificamos)
    'salário': 'salario',
    'valorsaláriofixo': 'salario',
    
    # DADOS PESSOAIS
    'raçacor': 'raca_cor',
    'sexo': 'sexo',
    'idade': 'idade',
    'graudeinstrução': 'grau_instrucao',
    'instrucao': 'grau_instrucao',
    
    # OUTROS
    'cbo2002ocupação': 'cbo2002',
    'categoria': 'categoria',
    'horascontratuais': 'horas_contratuais',
    'tipoempregador': 'tipo_empregador',
    'tipoestabelecimento': 'tipo_estabelecimento',
    'indtrabalhointermitente': 'ind_trabalho_intermitente',
    'indtrabalhoparcial': 'ind_trabalho_parcial',
    
    # CAMPOS GERADOS PELO SISTEMA (Join)
    'secao_descricao': 'secao_descricao',
    'atividade_economica': 'atividade_economica'
}