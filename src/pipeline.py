import os
import logging
import gc
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    from .config import RAW_ZIP_DIR, FTP_HOST, FTP_BASE_PATH
    from .ftp_client import FTPClient
    from .processor import CagedProcessor
    from .loader import load_to_database
except ImportError:
    from src.config import RAW_ZIP_DIR, FTP_HOST, FTP_BASE_PATH
    from src.ftp_client import FTPClient
    from src.processor import CagedProcessor
    from src.loader import load_to_database

def run_pipeline(year: str, month: str, force_download=False):
    """
    Executa o pipeline ETL completo.
    """
    filename = f"CAGEDMOV{year}{month}.7z"
    logger.info(f"=== 🏁 INICIANDO PIPELINE CAGED (SRC): {month}/{year} ===")

    ftp = FTPClient(FTP_HOST)
    processor = CagedProcessor()
    
    try:
        # 1. Extração
        logger.info("[1/3] Extração (Download FTP)...")
        ftp.connect()
        
        folder_month = f"{year}{month}"
        remote_path = f"{FTP_BASE_PATH}{year}/{folder_month}/"
        
        zip_path = ftp.download_file(remote_path, filename, RAW_ZIP_DIR)
        ftp.close()

        if zip_path is None:
            logger.warning(f"⚠️ PIPELINE INTERROMPIDO: Arquivo não encontrado.")
            return 

        # 2. Transformação
        logger.info("[2/3] Transformação (ETL + Join)...")
        txt_path = processor.extract_file(zip_path)
        
        if not txt_path:
            raise Exception("Falha na extração do 7z.")

        csv_filename = f"CAGED_SP_{year}_{month}.csv"
        
        # O process_data retorna o CAMINHO COMPLETO do CSV gerado
        final_csv_path = processor.process_data(txt_path, csv_filename, year, month)

        # 3. Carga
        logger.info("[3/3] Carga (Banco de Dados)...")
        
        # --- CORREÇÃO AQUI: Passa o caminho completo, não só o nome ---
        if load_to_database(final_csv_path):
            logger.info(f"=== ✨ SUCESSO! Dados de {month}/{year} carregados. ===")
        else:
            logger.error("❌ Falha na carga do banco.")
            raise Exception("Erro no Loader.")
        # --------------------------------------------------------------

        # Limpeza
        logger.info("🧹 Limpando arquivos temporários...")
        try:
            if os.path.exists(txt_path): os.remove(txt_path)
            path_utf8 = txt_path + ".utf8"
            if os.path.exists(path_utf8): os.remove(path_utf8)
        except: pass

    except Exception as e:
        logger.error(f"🔥 Erro no Pipeline: {e}")
        raise e
    finally:
        if 'ftp' in locals(): ftp.close()
        gc.collect() 
        time.sleep(2)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=str)
    parser.add_argument('--month', type=str)
    args = parser.parse_args()
    if args.year and args.month:
        run_pipeline(args.year, args.month)