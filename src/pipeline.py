import os
import logging
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    from .ftp_client import FTPClient
    from .processor import CagedProcessor
    from .loader import load_to_database
    from .config import REFS_DIR
except ImportError:
    from src.ftp_client import FTPClient
    from src.processor import CagedProcessor
    from src.loader import load_to_database
    from src.config import REFS_DIR

def check_setup():
    required = ['municipios.csv', 'cnae.csv']
    missing = [f for f in required if not os.path.exists(os.path.join(REFS_DIR, f))]
    if missing:
        logger.warning(f"⚠️ Faltam arquivos de referência: {missing} (Joins podem falhar)")
    return True

def run_pipeline(year: str, month: str):
    logger.info(f"=== 🏁 INICIANDO PIPELINE CAGED (SRC): {month}/{year} ===")
    check_setup()
    txt_path = None
    
    try:
        # 1. Extract
        logger.info("[1/3] Extração (Download FTP)...")
        ftp = FTPClient()
        zip_path = ftp.download_caged(year, month)
        if not zip_path: return

        # 2. Transform
        logger.info("[2/3] Transformação (ETL + Join)...")
        processor = CagedProcessor()
        txt_path = processor.extract_file(zip_path)
        if not txt_path: return
        
        csv_filename = f"CAGED_SP_{year}_{month.zfill(2)}.csv"
        processor.process_data(txt_path, csv_filename, year, month)

        # 3. Load
        logger.info("[3/3] Carga (Banco de Dados)...")
        load_to_database(csv_filename)
        logger.info(f"=== ✨ SUCESSO! Dados de {month}/{year} carregados. ===")

    except Exception as e:
        logger.error(f"🔥 Ocorreu um erro não tratado no pipeline: {e}")
        raise e
    finally:
        logger.info("🧹 Executando limpeza de temporários...")
        if txt_path and os.path.exists(txt_path):
            try: os.remove(txt_path)
            except: pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=str)
    parser.add_argument('--month', type=str)
    args = parser.parse_args()
    if args.year and args.month:
        run_pipeline(args.year, args.month)