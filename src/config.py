import os

# --- DIRETÓRIOS ---
SRC_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SRC_DIR)
DATA_DIR_SRC = os.path.join(SRC_DIR, 'data')

RAW_ZIP_DIR = os.path.join(BASE_DIR, 'data', 'raw')
PROCESSED_DIR = os.path.join(BASE_DIR, 'data', 'processed')
REFS_DIR = os.path.join(DATA_DIR_SRC, 'refs')

# Garante diretórios
os.makedirs(PROCESSED_DIR, exist_ok=True)

# --- ARQUIVOS DE REFERÊNCIA (De/Para) ---
# O sistema vai procurar por estes arquivos para trazer as descrições
REFS_FILES = {
    "municipios": os.path.join(REFS_DIR, "municipios.csv"),
    "secoes": os.path.join(REFS_DIR, "secoes.csv"),
    "subclasse": os.path.join(REFS_DIR, "cnae.csv"), # CNAE completo
    "categoria": os.path.join(REFS_DIR, "categoria.csv"),
    "grau_instrucao": os.path.join(REFS_DIR, "grau_instrucao.csv"),
    "tipo_movimentacao": os.path.join(REFS_DIR, "tipo_movimentacao.csv"),
    "raca": os.path.join(REFS_DIR, "raca.csv"),
    "sexo": os.path.join(REFS_DIR, "sexo.csv")
}

# --- CONFIGURAÇÕES GERAIS ---
FTP_HOST = "ftp.mtps.gov.br"
FTP_BASE_PATH = "/pdet/microdados/NOVO CAGED/"
UF_FILTER = 35 

# --- BANCO DE DADOS ---
DB_HOST = "postgres" 
DB_PORT = "5432"
DB_NAME = "airflow"
DB_USER = "airflow"
DB_PASS = "airflow"
TABLE_NAME = "caged_sp_completo" # Mudei o nome para refletir que agora é completo
DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- MAPEAMENTO INICIAL (TXT -> POLARS) ---
COLUMNS_MAP = {
    'competência': 'competencia',
    'região': 'regiao',
    'uf': 'uf_codigo',
    'município': 'municipio_codigo',
    'seção': 'secao_codigo',
    'subclasse': 'subclasse_codigo',
    'saldomovimentação': 'saldo_movimentacao',
    'tipomovimentação': 'tipo_movimentacao_codigo',
    'valorsaláriofixo': 'salario',
    'cbo2002ocupação': 'cbo_2002',
    'categoria': 'categoria_codigo',
    'sexo': 'sexo_codigo',
    'idade': 'idade',
    'graudeinstrução': 'grau_instrucao_codigo',
    'raçacor': 'raca_cor_codigo',
    'indtrabintermitente': 'trab_intermitente',
    'indtrabparcial': 'trab_parcial',
    'tamestabjan': 'tamanho_estab'
}