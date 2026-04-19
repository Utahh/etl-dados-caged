import ftplib
import os
import logging

try:
    from .config import FTP_HOST, FTP_BASE_PATH
except ImportError:
    from src.config import FTP_HOST, FTP_BASE_PATH

logger = logging.getLogger(__name__)

class FTPClient:
    def __init__(self):
        self.host = FTP_HOST
        self.base_path = FTP_BASE_PATH
        self.ftp = None

    def connect(self):
        try:
            logger.info(f"🔌 Conectando ao FTP: {self.host}...")
            self.ftp = ftplib.FTP(self.host)
            self.ftp.login()
            logger.info("✅ Conexão OK!")
        except Exception as e:
            logger.error(f"❌ Erro conexão FTP: {e}"); raise e

    def is_valid_7z(self, file_path):
        """Verifica se o arquivo é realmente um 7z olhando o cabeçalho (Magic Bytes)"""
        if not os.path.exists(file_path): return False
        if os.path.getsize(file_path) < 1000: return False # Muito pequeno
        
        try:
            with open(file_path, 'rb') as f:
                header = f.read(6)
                # Assinatura do 7zip: 37 7A BC AF 27 1C
                return header == b'\x37\x7a\xbc\xaf\x27\x1c'
        except:
            return False

    def download_file(self, year, month, file_type, output_dir, force=False):
        filename_options = [
            f"{file_type}{year}{month}.7z",
            f"{file_type}{year}{month}.7z",
            f"{file_type}{year}{month}.txt.7z"
        ]
        
        # 1. Checa Cache
        for fname in filename_options:
            path = os.path.join(output_dir, fname)
            if os.path.exists(path):
                if self.is_valid_7z(path):
                    if not force:
                        logger.info(f"📂 Cache válido encontrado: {fname}")
                        return path
                else:
                    logger.warning(f"⚠️ Arquivo corrompido ou falso no cache: {fname}. Deletando...")
                    os.remove(path)

        if not self.ftp: self.connect()

        # 2. Navega e Busca
        ftp_dir = f"{self.base_path}{year}/{year}{month}/"
        try:
            self.ftp.cwd(ftp_dir)
        except:
            logger.warning(f"Pasta {ftp_dir} não existe.")
            return None

        try: files_list = self.ftp.nlst()
        except: files_list = []

        found_remote = None
        for remote in files_list:
            if file_type.upper() in remote.upper() and f"{year}{month}" in remote and remote.endswith(".7z"):
                found_remote = remote
                break
        
        if not found_remote:
            logger.warning(f"Arquivo {file_type} não encontrado no FTP.")
            return None

        # 3. Download
        local_path = os.path.join(output_dir, found_remote)
        logger.info(f"⬇️ Baixando {found_remote}...")
        
        try:
            with open(local_path, 'wb') as f:
                self.ftp.retrbinary(f"RETR {found_remote}", f.write)
            
            if self.is_valid_7z(local_path):
                logger.info("✅ Download e validação OK!")
                return local_path
            else:
                logger.error("❌ O arquivo baixado não é um 7zip válido (provavelmente erro do servidor).")
                os.remove(local_path)
                return None

        except Exception as e:
            logger.error(f"❌ Erro download: {e}")
            if os.path.exists(local_path): os.remove(local_path)
            self.close()
            return None

    def close(self):
        if self.ftp:
            try: self.ftp.quit()
            except: pass
            self.ftp = None