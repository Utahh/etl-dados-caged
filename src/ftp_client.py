import os
import ftplib
import logging
from .config import FTP_HOST, FTP_BASE_PATH, RAW_ZIP_DIR, MAX_HISTORY_FILES

logger = logging.getLogger(__name__)

class FTPClient:
    def download_caged(self, year: str, month: str):
        filename = f"CAGEDMOV{year}{month}.7z"
        local_path = os.path.join(RAW_ZIP_DIR, filename)
        
        # 1. Verifica se já existe
        if os.path.exists(local_path):
            logger.info(f"ℹ️ Arquivo {filename} já existe no histórico. Pulando download.")
            return local_path
            
        # 2. Limpeza de histórico (Mantém apenas os últimos X arquivos)
        try:
            files = sorted(
                [os.path.join(RAW_ZIP_DIR, f) for f in os.listdir(RAW_ZIP_DIR)],
                key=os.path.getmtime
            )
            while len(files) >= MAX_HISTORY_FILES:
                oldest = files.pop(0)
                os.remove(oldest)
                logger.info(f"🧹 Histórico: Removido arquivo antigo {os.path.basename(oldest)}")
        except Exception as e:
            logger.warning(f"⚠️ Erro ao limpar histórico: {e}")

        # 3. Download
        ftp = ftplib.FTP(FTP_HOST)
        try:
            logger.info("🔌 Conectando ao FTP...")
            ftp.login()
            
            # Tenta navegar para o diretório do ano
            path = f"{FTP_BASE_PATH}/{year}/{year}{month}"
            # As vezes o governo muda o caminho (sem a barra extra ou com), tenta ajustar se falhar
            try:
                ftp.cwd(path)
            except:
                # Tentativa de fallback (caminho antigo)
                path = f"{FTP_BASE_PATH}/{year}"
                ftp.cwd(path)

            logger.info(f"📂 Navegando: {path}")
            logger.info(f"⬇️ Baixando {filename} para {RAW_ZIP_DIR}...")
            
            with open(local_path, 'wb') as f:
                ftp.retrbinary(f"RETR {filename}", f.write)
                
            logger.info("✅ Download concluído!")
            return local_path
            
        except Exception as e:
            logger.error(f"❌ Erro no FTP: {e}")
            if os.path.exists(local_path):
                os.remove(local_path) # Remove arquivo corrompido
            return None
        finally:
            try: ftp.quit()
            except: pass