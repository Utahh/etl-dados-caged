import os

# --- DIRETÓRIOS DO PROJETO ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(os.path.dirname(BASE_DIR), "data")

RAW_ZIP_DIR = os.path.join(DATA_DIR, "raw_zips")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
REFS_DIR = os.path.join(DATA_DIR, "refs")

# Cria as pastas automaticamente
os.makedirs(RAW_ZIP_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(REFS_DIR, exist_ok=True)

# --- CONEXÃO COM O BANCO ---
DB_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
TABLE_NAME = "caged_sp_completo"

# --- CONFIGURAÇÃO FTP ---
FTP_HOST = "ftp.mtps.gov.br"
FTP_BASE_PATH = "/pdet/microdados/NOVO CAGED/"

# --- ARQUIVOS DE REFERÊNCIA (Devolvido para compatibilidade) ---
REFS_FILES = {
    "municipios": os.path.join(REFS_DIR, "municipios.csv"),
    "secoes": os.path.join(REFS_DIR, "secoes.csv"),
}

# --- MAPEAMENTO DE COLUNAS ---
# A chave (esquerda) deve estar em minúsculo e sem acento (o Processor garante isso).
COLUMNS_MAP = {
    # Identificação
    "competenciamov": "competencia_mov",
    "regiao": "regiao",
    "uf": "uf_codigo",
    "municipio": "municipio_codigo",
    
    # Atividade
    "secao": "secao_codigo",
    "subclasse": "subclasse_codigo",
    
    # Movimentação
    "saldomovimentacao": "saldo_movimentacao",
    "cbo2002ocupacao": "cbo2002_ocupacao",
    "tipomovimentacao": "tipo_movimentacao_codigo",
    "categoria": "categoria_codigo",
    
    # Perfil
    "graudeinstrucao": "grau_instrucao_codigo",
    "idade": "idade",
    "racacor": "raca_cor_codigo",
    "sexo": "sexo_codigo",
    "tipodedeficiencia": "tipo_deficiencia",
    
    # Contrato
    "tipoempregador": "tipo_empregador",
    "tipoestabelecimento": "tipo_estabelecimento",
    "horascontratuais": "horascontratuais",
    "salario": "salario",
    "indtrabintermitente": "ind_trab_intermitente",
    "indtrabparcial": "ind_trab_parcial",
    "tamestabjan": "tam_estab_jan",
    "indicadoraprendiz": "indicador_aprendiz",
    "origemdainformacao": "origem_informacao",
    
    # Exclusão / Fora do Prazo
    "competenciadec": "competencia_dec",
    "competenciaexc": "competencia_exc",
    "indicadordeexclusao": "indicador_exclusao",
    "indicadorexclusao": "indicador_exclusao",
    "indicadordeforadoprazo": "indicador_fora_prazo",
    "unidadesalariocodigo": "unidade_salario_codigo",
    "valorsalariofixo": "valor_salario_fixo"
}