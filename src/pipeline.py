import os
import logging
from sqlalchemy import create_engine, text
from datetime import datetime

try:
    from .config import DB_URL, RAW_ZIP_DIR, REFS_FILES
    from .ftp_client import FTPClient
    from .processor import CagedProcessor
    from .loader import load_to_database
except ImportError:
    from src.config import DB_URL, RAW_ZIP_DIR, REFS_FILES
    from src.ftp_client import FTPClient
    from src.processor import CagedProcessor
    from src.loader import load_to_database

logger = logging.getLogger(__name__)

def get_real_col_name(conn, table, possible_names):
    """Descobre qual nome de coluna realmente existe no banco."""
    # Essa função evita erros se o nome mudar ligeiramente
    from sqlalchemy import inspect
    inspector = inspect(conn)
    columns = [c['name'] for c in inspector.get_columns(table)]
    for name in possible_names:
        if name in columns:
            return name
    return possible_names[0] # Retorna o padrão se não achar

def refresh_sql_model():
    """
    Roda após a carga para atualizar Tabelas Dimensão e Índices.
    Essencial para o Power BI.
    """
    logger.info("🏗️ Atualizando Modelo de Dados (Star Schema)...")
    engine = create_engine(DB_URL)
    
    with engine.begin() as conn:
        # 1. Cria Índices de Performance (Deixa o Power BI rápido)
        # Índice na Data (Para filtros de tempo)
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_caged_data_ref ON caged_sp_completo (data_ref);"))
        # Índice no Município (Para mapas)
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_caged_municipio ON caged_sp_completo (municipio_codigo);"))
        
        # 2. Cria Tabela Dimensão: Localidade (Municípios únicos)
        logger.info("📍 Atualizando dLocalidade...")
        conn.execute(text("DROP TABLE IF EXISTS dLocalidade CASCADE;"))
        conn.execute(text("""
            CREATE TABLE dLocalidade AS 
            SELECT DISTINCT municipio_codigo 
            FROM caged_sp_completo 
            WHERE municipio_codigo IS NOT NULL;
        """))
        
        # 3. Cria Tabela Dimensão: Atividades (Seções/CNAE)
        logger.info("🏭 Atualizando dAtividades...")
        conn.execute(text("DROP TABLE IF EXISTS dAtividades CASCADE;"))
        conn.execute(text("""
            CREATE TABLE dAtividades AS 
            SELECT DISTINCT secao_codigo, subclasse_codigo
            FROM caged_sp_completo 
            WHERE secao_codigo IS NOT NULL;
        """))

    logger.info("✅ Modelo de dados atualizado!")

def run_pipeline(year, month, force_download=False):
    processor = CagedProcessor()
    ftp = FTPClient()
    
    # Lista de arquivos esperados (MOV, FOR, EXC)
    # Nota: O layout do nome do arquivo muda as vezes, o FTPClient busca pelo padrão
    files_to_process = ["CAGEDMOV", "CAGEDFOR", "CAGEDEXC"]
    
    for ftype in files_to_process:
        try:
            # 1. Download
            local_path = ftp.download_file(year, month, ftype, RAW_ZIP_DIR, force=force_download)
            if not local_path:
                logger.warning(f"⚠️ Arquivo {ftype} de {month}/{year} não encontrado. Pulando.")
                continue

            # 2. Extração
            txt_path = processor.extract_file(local_path)
            if not txt_path:
                logger.error(f"❌ Falha ao extrair {local_path}")
                continue

            # 3. Processamento (Limpeza + Datas)
            csv_filename = f"{ftype}_{year}{month}_sp.csv"
            processed_path = processor.process_data(txt_path, csv_filename, year, month, ftype)

            # 4. Carga no Banco (Loader)
            # Passamos year-month-01 para compor a chave de exclusão (data_arquivo)
            data_ref_iso = f"{year}-{month}-01"
            
            if load_to_database(processed_path, data_ref_iso, ftype):
                logger.info(f"✅ {ftype} carregado com sucesso!")
            
            # Limpeza do arquivo temporário extraído (economiza espaço)
            if os.path.exists(txt_path): os.remove(txt_path)

        except Exception as e:
            logger.error(f"❌ Erro no pipeline de {ftype}: {e}")
            raise e
    
    # 5. Atualiza as Dimensões e Índices no final
    refresh_sql_model()