import os
import ftplib
import logging

# Ajuste de importação para evitar erros
try:
    from .config import FTP_HOST
except ImportError:
    from src.config import FTP_HOST

logger = logging.getLogger(__name__)

class FTPClient:
    def __init__(self, host=None):
        # Se não passar host, usa o do config
        self.host = host or FTP_HOST
        self.ftp = None

    def connect(self):
        """Estabelece conexão com o FTP"""
        try:
            self.ftp = ftplib.FTP(self.host)
            self.ftp.login()
            logger.info(f"✅ Conectado ao FTP: {self.host}")
        except Exception as e:
            logger.error(f"❌ Erro de conexão FTP: {e}")
            raise e

    def download_file(self, remote_path, filename, local_dir):
        """
        Baixa um arquivo do FTP para o diretório local.
        """
        local_path = os.path.join(local_dir, filename)
        
        try:
            # Garante que o diretório local existe
            os.makedirs(local_dir, exist_ok=True)
            
            # Muda para a pasta remota
            logger.info(f"📂 Acessando pasta remota: {remote_path}")
            self.ftp.cwd(remote_path)
            
            # Baixa o arquivo
            logger.info(f"⬇️ Baixando {filename} para {local_path}...")
            with open(local_path, "wb") as f:
                self.ftp.retrbinary(f"RETR {filename}", f.write)
            
            logger.info(f"✅ Download concluído: {local_path}")
            return local_path
            
        except Exception as e:
            logger.error(f"❌ Falha ao baixar {filename}: {e}")
            # Remove arquivo parcial se der erro
            if os.path.exists(local_path):
                os.remove(local_path)
            return None

    def close(self):
        """Fecha a conexão de forma segura"""
        if self.ftp:
            try:
                self.ftp.quit()
            except:
                try:
                    self.ftp.close()
                except:
                    pass