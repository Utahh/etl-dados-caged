import os
import glob
import logging
from sqlalchemy import create_engine, text

try:
    # Ajuste de importação para não depender obrigatoriamente de REFS_FILES
    from .config import DB_URL, RAW_ZIP_DIR, TABLE_NAME, PROCESSED_DIR
    from .ftp_client import FTPClient
    from .processor import CagedProcessor
    from .loader import load_to_database
except ImportError:
    from src.config import DB_URL, RAW_ZIP_DIR, TABLE_NAME, PROCESSED_DIR
    from src.ftp_client import FTPClient
    from src.processor import CagedProcessor
    from src.loader import load_to_database

logger = logging.getLogger(__name__)

def refresh_sql_model():
    """
    Roda apenas UMA VEZ ao final de toda a carga.
    Cria tabelas auxiliares (Dimensões) e índices para o Power BI.
    """
    logger.info("🏗️ Criando Índices e Tabelas Dimensão (Otimização Final)...")
    engine = create_engine(DB_URL)
    
    with engine.begin() as conn:
        # 1. Índices de Performance
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_data_ref ON {TABLE_NAME} (data_ref);"))
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_municipio ON {TABLE_NAME} (municipio_codigo);"))
        
        # 2. Dimensão Localidade (Municípios únicos)
        conn.execute(text("DROP TABLE IF EXISTS dLocalidade CASCADE;"))
        conn.execute(text(f"""
            CREATE TABLE dLocalidade AS 
            SELECT DISTINCT municipio_codigo 
            FROM {TABLE_NAME} 
            WHERE municipio_codigo IS NOT NULL;
        """))
        
        # 3. Dimensão Atividades (Seções CNAE únicas)
        conn.execute(text("DROP TABLE IF EXISTS dAtividades CASCADE;"))
        conn.execute(text(f"""
            CREATE TABLE dAtividades AS 
            SELECT DISTINCT secao_codigo 
            FROM {TABLE_NAME} 
            WHERE secao_codigo IS NOT NULL;
        """))

    logger.info("✅ Tudo pronto! O banco está otimizado para o Power BI.")

def cleanup_stale_files(keep_last=2):
    """
    Mantém apenas os 'keep_last' arquivos mais recentes na pasta processed.
    Evita lotar o disco do Docker.
    """
    types = ["CAGEDMOV", "CAGEDFOR", "CAGEDEXC"]
    
    for ftype in types:
        pattern = os.path.join(PROCESSED_DIR, f"{ftype}_*.csv")
        files = glob.glob(pattern)
        files.sort(reverse=True) # Mais novos primeiro
        
        if len(files) > keep_last:
            for f in files[keep_last:]:
                try:
                    os.remove(f)
                    logger.info(f"🧹 Faxina: Removido {os.path.basename(f)}")
                except OSError as e:
                    logger.warning(f"⚠️ Erro ao deletar {f}: {e}")

def run_pipeline(year, month, force_download=False):
    processor = CagedProcessor()
    ftp = FTPClient()
    
    files_to_process = ["CAGEDMOV", "CAGEDFOR", "CAGEDEXC"]
    
    for ftype in files_to_process:
        try:
            # 1. Download
            local_path = ftp.download_file(year, month, ftype, RAW_ZIP_DIR, force=force_download)
            if not local_path: continue # Se não achar no FTP, segue o jogo

            # 2. Extração
            txt_path = processor.extract_file(local_path)
            if not txt_path: continue

            # 3. Processamento
            csv_filename = f"{ftype}_{year}{month}_sp.csv"
            processed_path = processor.process_data(txt_path, csv_filename, year, month, ftype)

            # 4. Carga no Banco
            data_ref_iso = f"{year}-{month}-01"
            
            # Chama o loader
            load_to_database(processed_path, data_ref_iso, ftype)
            
            # Limpa o arquivo TXT bruto extraído para economizar espaço imediato
            if os.path.exists(txt_path): os.remove(txt_path)

        except Exception as e:
            logger.error(f"❌ Falha crítica em {ftype}: {e}")
            raise e

    # 5. Faxina da pasta Processed (opcional, roda ao final do mês)
    cleanup_stale_files(keep_last=2)