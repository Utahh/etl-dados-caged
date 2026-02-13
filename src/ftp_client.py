import ftplib
import os
import logging
from time import sleep

try:
    from .config import FTP_HOST, FTP_BASE_PATH
except ImportError:
    from src.config import FTP_HOST, FTP_BASE_PATH

logger = logging.getLogger(__name__)

class FTPClient:
    def __init__(self):
        self.ftp = None

    def _ensure_connection(self):
        """Garante que a conexão está ativa, conectando se for a primeira vez ou reconectando se caiu."""
        # 1. Tenta verificar se a conexão atual está viva
        if self.ftp:
            try:
                self.ftp.voidcmd("NOOP")
                return # Está viva, pode sair
            except:
                logger.warning("⚠️ Conexão FTP caiu ou instável. Reiniciando...")
                self.ftp = None # Marca como morta para reconectar abaixo

        # 2. Se chegou aqui, é porque self.ftp é None OU a conexão caiu
        try:
            logger.info(f"🔌 Conectando ao FTP: {FTP_HOST}...")
            self.ftp = ftplib.FTP(FTP_HOST)
            self.ftp.login()
            logger.info("✅ Conexão estabelecida com sucesso!")
        except Exception as e:
            logger.error(f"❌ Falha crítica ao conectar no FTP: {e}")
            raise e

    def _find_file_in_ftp(self, path, prefix):
        """
        Procura um arquivo no FTP que comece com o prefixo.
        """
        try:
            self._ensure_connection() # Garante conexão antes de tentar listar
            self.ftp.cwd(path)
            files = self.ftp.nlst()
            for f in files:
                if f.upper().startswith(prefix.upper()) and f.endswith(".7z"):
                    return f
            return None
        except Exception as e:
            # Não vamos logar erro crítico aqui para não sujar o log se o arquivo só não existir
            # Mas se for erro de conexão, o _ensure_connection já teria pegado
            logger.warning(f"⚠️ Não foi possível listar arquivos em {path}: {e}")
            return None

    def download_file(self, year, month, file_type, output_dir, force=False):
        """
        Baixa o arquivo do FTP.
        """
        self._ensure_connection()
        
        ftp_dir = f"{FTP_BASE_PATH}{year}/{year}{month}/"
        file_prefix = f"{file_type}{year}{month}"
        
        # Tenta achar o arquivo
        filename = self._find_file_in_ftp(ftp_dir, file_prefix)
        
        if not filename:
            logger.warning(f"⚠️ Arquivo não encontrado no FTP: {ftp_dir}{file_prefix}*")
            return None

        local_path = os.path.join(output_dir, filename)

        if os.path.exists(local_path) and not force:
            logger.info(f"📂 Arquivo já existe em cache: {filename}")
            return local_path

        logger.info(f"⬇️ Baixando: {filename}...")
        
        try:
            with open(local_path, 'wb') as f:
                self.ftp.retrbinary(f"RETR {filename}", f.write)
            logger.info("✅ Download concluído!")
            return local_path
        except Exception as e:
            if os.path.exists(local_path):
                os.remove(local_path)
            logger.error(f"❌ Erro no download: {e}")
            raise e

    def close(self):
        if self.ftp:
            try:
                self.ftp.quit()
            except:
                self.ftp.close()