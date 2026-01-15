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
# Se estiver rodando dentro do Docker (Airflow), usa o service name 'postgres'
# Se for rodar localmente para testes, use 'localhost'
DB_HOST = "postgres" 
DB_PORT = "5432"
DB_NAME = "airflow"
DB_USER = "airflow"
DB_PASS = "airflow"
TABLE_NAME = "caged_sp_novo"

# URL de Conexão SQLAlchemy
DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- MAPEAMENTO DE COLUNAS (CRUCIAL) ---
# A chave (esquerda) é como vem no arquivo TXT/CSV do governo.
# O valor (direita) é como será salvo no seu Banco de Dados.
COLUMNS_MAP = {
    # --- MUNICÍPIO ---
    'município': 'municipio_codigo',
    'municipio_codigo': 'municipio_codigo',
    'municapio': 'municipio_codigo',
    'municipio': 'municipio_codigo',

    # --- SEÇÃO / SETOR (Onde estava o problema) ---
    # Adicionamos todas as variações possíveis para garantir que ele pegue
    'seção': 'secao_descricao',
    'secao': 'secao_descricao',
    'secao_codigo': 'secao_codigo', # Código (A, B, C...)
    
    'secao_nome': 'secao_descricao',       # Nome padrão (INDÚSTRIA...)
    'seção_nome': 'secao_descricao',       # Com acento
    'seçao_nome': 'secao_descricao',       # Erro comum
    'seaaonome': 'secao_descricao',        # Erro de encoding comum
    'secao_descricao': 'secao_descricao',

    # --- SUBCLASSE (Atividade Específica) ---
    'subclasse': 'subclasse_codigo',
    'subclasse_codigo': 'subclasse_codigo',
    'subclasse_descricao': 'subclasse_descricao',
    'subclasse_desc': 'subclasse_descricao',
    'subclassedescricao': 'subclasse_descricao',

    # --- MOVIMENTAÇÃO E SALDO ---
    'competênciamov': 'competencia_mov',
    'competencia_mov': 'competencia_mov',
    'competaanciamov': 'competencia_mov',
    
    'saldo': 'saldo_movimentacao',
    'saldo_movimentacao': 'saldo_movimentacao',
    'saldomovimentaaao': 'saldo_movimentacao',
    
    'tipomovimentação': 'tipo_movimentacao',
    'tipo_movimentacao_codigo': 'tipo_movimentacao',
    'tipomovimentaaao': 'tipo_movimentacao',

    # --- SALÁRIO ---
    'salário': 'salario',
    'salario': 'salario',
    'valorsaláriofixo': 'salario',
    'valorsalariofixo': 'salario',

    # --- DADOS DEMOGRÁFICOS ---
    'raçacor': 'raca_cor',
    'raca_cor_codigo': 'raca_cor',
    'raaacor': 'raca_cor',
    
    'sexo': 'sexo',
    'sexo_codigo': 'sexo',
    
    'idade': 'idade',
    
    'graudeinstrução': 'grau_instrucao',
    'grau_instrucao_codigo': 'grau_instrucao',
    'graudeinstruaao': 'grau_instrucao',

    # --- OUTROS ---
    'cbo2002ocupação': 'cbo2002',
    'cbo2002ocupaaao': 'cbo2002',
    'cbo2002': 'cbo2002',
    'categoria': 'categoria',
    'horascontratuais': 'horas_contratuais',
    'tipoempregador': 'tipo_empregador',
    'tipoestabelecimento': 'tipo_estabelecimento',
    'indtrabalhointermitente': 'ind_trabalho_intermitente',
    'indtrabalhoparcial': 'ind_trabalho_parcial',
    
    # --- CAMPO NOVO (Gerado pelo Python) ---
    'atividade_economica': 'atividade_economica'
}