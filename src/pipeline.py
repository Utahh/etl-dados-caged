import os
import logging
from sqlalchemy import create_engine, text

try:
    from .config import DB_URL, RAW_ZIP_DIR, FTP_HOST, FTP_BASE_PATH, PROCESSED_DIR
    from .loader import load_to_database
    from .ftp_client import FTPClient
    from .processor import CagedProcessor
except ImportError:
    from src.config import DB_URL, RAW_ZIP_DIR, FTP_HOST, FTP_BASE_PATH, PROCESSED_DIR
    from src.loader import load_to_database
    from src.ftp_client import FTPClient
    from src.processor import CagedProcessor

logger = logging.getLogger(__name__)

def refresh_sql_model():
    """Cria as tabelas auxiliares baseadas na tabela principal limpa."""
    logger.info("🏗️ Atualizando fotos das dimensões...")
    engine = create_engine(DB_URL)
    
    sqls = [
        "DROP TABLE IF EXISTS dLocalidade, dSexo, dRaca, dEscolaridade, fMovimentacoes CASCADE;",
        
        # Agora usamos os nomes CERTOS que definimos no CONFIG.PY
        "CREATE TABLE dLocalidade AS SELECT DISTINCT municipio_codigo FROM caged_sp_completo WHERE municipio_codigo IS NOT NULL;",
        "CREATE TABLE dSexo AS SELECT DISTINCT sexo_codigo FROM caged_sp_completo WHERE sexo_codigo IS NOT NULL;",
        "CREATE TABLE dRaca AS SELECT DISTINCT raca_cor_codigo FROM caged_sp_completo WHERE raca_cor_codigo IS NOT NULL;",
        
        # A Tabela Fato é apenas um espelho da principal, pois já tratamos tudo no processor
        "CREATE TABLE fMovimentacoes AS SELECT * FROM caged_sp_completo;"
    ]
    
    with engine.begin() as conn:
        for sql in sqls:
            conn.execute(text(sql))
    logger.info("✅ Modelo de dados atualizado!")

def run_pipeline(year, month, force_download=False):
    ftp = FTPClient(); proc = CagedProcessor()
    file_types = ["CAGEDMOV", "CAGEDFOR", "CAGEDEXC"]
    files_loaded = 0
    
    # Limpa processados anteriores
    if os.path.exists(PROCESSED_DIR):
        for f in os.listdir(PROCESSED_DIR):
            if f.endswith(".csv"): os.remove(os.path.join(PROCESSED_DIR, f))

    ftp.connect()
    for ftype in file_types:
        fname = f"{ftype}{year}{month}.7z"
        local_zip = os.path.join(RAW_ZIP_DIR, fname)
        if not os.path.exists(local_zip) or force_download:
            local_zip = ftp.download_file(f"{FTP_BASE_PATH}{year}/{year}{month}/", fname, RAW_ZIP_DIR)
        
        if local_zip:
            txt = proc.extract_file(local_zip)
            if txt:
                # Processa e Renomeia Colunas
                csv = proc.process_data(txt, f"{ftype}_{year}_{month}.csv", year, month, ftype)
                
                # Carrega no Banco (O loader vai criar a tabela com os nomes NOVOS e LIMPOS)
                if load_to_database(csv, f"{year}-{month}-01", ftype):
                    files_loaded += 1
                
                if os.path.exists(txt): os.remove(txt)
    ftp.close()
    
    # Gera o sample com nomes descritivos
    proc.generate_sample_file()
    
    if files_loaded > 0:
        refresh_sql_model()