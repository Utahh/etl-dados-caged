import os
import ftplib
import logging
from .config import FTP_HOST, FTP_BASE_PATH, RAW_ZIP_DIR, MAX_HISTORY_FILES

logger = logging.getLogger(__name__)

class FTPClient:
    def _connect(self):
        ftp = ftplib.FTP(FTP_HOST)
        # Força Latin-1 para aceitar acentos do governo
        ftp.encoding = 'latin-1' 
        ftp.login()
        return ftp

    def get_available_periods(self, limit=3):
        ftp = self._connect()
        logger.info("📡 Sondando FTP por dados disponíveis...")
        available_periods = []
        try:
            ftp.cwd(FTP_BASE_PATH)
            years = [y for y in ftp.nlst() if y.isdigit() and len(y) == 4]
            years.sort(reverse=True)
            
            for year in years:
                if len(available_periods) >= limit: break
                try:
                    ftp.cwd(f"{FTP_BASE_PATH}/{year}")
                    items = ftp.nlst()
                    months_found = []
                    for item in items:
                        if item.isdigit() and len(item) == 6 and item.startswith(year):
                            months_found.append(item[4:6])
                    months_found.sort(reverse=True)
                    for m in months_found:
                        available_periods.append((year, m))
                        if len(available_periods) >= limit: break
                except: continue
        except Exception as e:
            logger.error(f"❌ Erro ao sondar FTP: {e}")
        finally:
            ftp.quit()
        logger.info(f"📅 Períodos encontrados: {available_periods}")
        return available_periods

    def download_caged(self, year: str, month: str):
        filename = f"CAGEDMOV{year}{month}.7z"
        local_path = os.path.join(RAW_ZIP_DIR, filename)
        
        # --- NOVO: Verificação de Integridade ---
        if os.path.exists(local_path):
            file_size = os.path.getsize(local_path)
            if file_size > 0:
                logger.info(f"ℹ️ Arquivo {filename} já existe ({file_size/1024:.2f} KB). Pulando download.")
                return local_path
            else:
                logger.warning(f"⚠️ Arquivo {filename} existe mas está VAZIO (0 bytes). Removendo para baixar de novo.")
                os.remove(local_path)
        # ----------------------------------------

        # Limpeza de histórico
        try:
            files = sorted(
                [os.path.join(RAW_ZIP_DIR, f) for f in os.listdir(RAW_ZIP_DIR)],
                key=os.path.getmtime
            )
            while len(files) >= MAX_HISTORY_FILES:
                oldest = files.pop(0)
                os.remove(oldest)
        except Exception: pass

        # Download
        ftp = self._connect()
        try:
            paths_to_try = [
                f"{FTP_BASE_PATH}/{year}/{year}{month}",
                f"{FTP_BASE_PATH}/{year}"
            ]
            success = False
            for path in paths_to_try:
                try:
                    ftp.cwd(path)
                    logger.info(f"⬇️ Baixando {filename} de {path}...")
                    with open(local_path, 'wb') as f:
                        ftp.retrbinary(f"RETR {filename}", f.write)
                    
                    # Verifica se baixou algo
                    if os.path.getsize(local_path) > 0:
                        success = True
                        break
                except: continue
            
            if success:
                logger.info("✅ Download concluído!")
                return local_path
            else:
                logger.error(f"❌ Arquivo {filename} não encontrado no FTP.")
                if os.path.exists(local_path): os.remove(local_path) # Limpa lixo
                return None
            
        except Exception as e:
            logger.error(f"❌ Erro crítico FTP: {e}")
            if os.path.exists(local_path): os.remove(local_path)
            return None
        finally:
            ftp.quit()