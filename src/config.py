import os

# --- DIRETÓRIOS DO PROJETO ---
# Garante que os caminhos funcionem tanto localmente quanto no Docker
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(os.path.dirname(BASE_DIR), "data")

RAW_ZIP_DIR = os.path.join(DATA_DIR, "raw_zips")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
REFS_DIR = os.path.join(DATA_DIR, "refs")

# Cria as pastas automaticamente se não existirem
os.makedirs(RAW_ZIP_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(REFS_DIR, exist_ok=True)

# --- CONFIGURAÇÕES DE CONEXÃO ---
# URL do Banco de Dados (Usuário:Senha@Host:Porta/Banco)
DB_URL = "postgresql://airflow:airflow@caged_postgres:5432/airflow"
TABLE_NAME = "caged_sp_completo"

# Configurações do FTP do Governo
FTP_HOST = "ftp.mtps.gov.br"
FTP_BASE_PATH = "/pdet/microdados/NOVO CAGED/"

# --- ARQUIVOS DE REFERÊNCIA (DESCRIÇÕES) ---
REFS_FILES = {
    "municipios": os.path.join(REFS_DIR, "municipios.csv"),
    "secoes": os.path.join(REFS_DIR, "secoes.csv"),
}

# --- MAPEAMENTO DE COLUNAS (CRUCIAL) ---
# Esquerda (Chave): O nome "sujo" que vem do arquivo (visto nos logs).
# Direita (Valor): O nome "limpo" que será criado no Banco de Dados.

COLUMNS_MAP = {
    # --- Identificação ---
    "competaanciamov": "competencia_mov",
    "regiao": "regiao",
    "uf": "uf_codigo",
    "uf_codigo": "uf_codigo",
    "municapio": "municipio_codigo",  # CORRIGIDO: Mapeia o erro de encoding
    
    # --- Atividade Econômica ---
    "seaao": "secao_codigo",          # CORRIGIDO: Mapeia o erro de encoding
    "subclasse": "subclasse_codigo",
    "subclasse_codigo": "subclasse_codigo",
    
    # --- Dados da Movimentação ---
    "saldomovimentaaao": "saldo_movimentacao", # CORRIGIDO
    "cbo2002ocupaaao": "cbo2002_ocupacao",
    "tipomovimentaaao": "tipo_movimentacao_codigo",
    "categoria": "categoria_codigo",
    "categoria_codigo": "categoria_codigo",
    
    # --- Perfil do Trabalhador ---
    "graudeinstruaao": "grau_instrucao_codigo",
    "idade": "idade",
    "raaacor": "raca_cor_codigo",     # CORRIGIDO
    "sexo": "sexo_codigo",
    "sexo_codigo": "sexo_codigo",
    "tipodedeficiaancia": "tipo_deficiencia",
    
    # --- Dados do Contrato ---
    "tipoempregador": "tipo_empregador",
    "tipoestabelecimento": "tipo_estabelecimento",
    "horas_contratuais": "horas_contratuais",
    "salario": "salario",
    "indtrabintermitente": "ind_trab_intermitente",
    "indtrabparcial": "ind_trab_parcial",
    "tamestabjan": "tam_estab_jan",
    "indicadoraprendiz": "indicador_aprendiz",
    "origemdainformaaao": "origem_informacao",
    
    # --- Campos Específicos de Arquivos FOR/EXC ---
    "competaanciadec": "competencia_dec",
    "competaanciaexc": "competencia_exc",
    "indicadordeexclusao": "indicador_exclusao",
    "indicadordeforadoprazo": "indicador_fora_prazo",
    "unidadesalarioca3digo": "unidade_salario_codigo",
    "valorsalariofixo": "valor_salario_fixo"
}