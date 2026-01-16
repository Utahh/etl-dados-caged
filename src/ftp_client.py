import ftplib
import os
import logging

logger = logging.getLogger(__name__)

# Importação Limpa (Sem variáveis desnecessárias)
try:
    from .config import FTP_HOST
except ImportError:
    from src.config import FTP_HOST

class FTPClient:
    def __init__(self, host, user='anonymous', passwd=''):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.ftp = None

    def connect(self):
        try:
            self.ftp = ftplib.FTP(self.host)
            self.ftp.login(self.user, self.passwd)
            self.ftp.encoding = 'latin-1' 
            return True
        except Exception as e:
            logger.error(f"❌ Erro ao conectar no FTP ({self.host}): {e}")
            raise e

    def close(self):
        if self.ftp:
            try: self.ftp.quit()
            except: pass

    def download_file(self, remote_path, filename, local_dir):
        """
        Baixa um arquivo específico.
        Retorna o caminho local se sucesso, ou None se falhar.
        """
        local_path = os.path.join(local_dir, filename)
        
        try:
            # 1. Tenta entrar na pasta remota
            try:
                self.ftp.cwd(remote_path)
            except ftplib.error_perm:
                logger.warning(f"⚠️ A pasta '{remote_path}' não existe no servidor.")
                return None
            
            # 2. Verifica se o arquivo existe na lista
            try:
                files_on_server = self.ftp.nlst()
            except ftplib.error_perm:
                files_on_server = []

            if filename not in files_on_server:
                logger.warning(f"⏳ Arquivo '{filename}' não encontrado em '{remote_path}'.")
                return None

            # 3. Verifica tamanho (evitar download repetido)
            try:
                remote_size = self.ftp.size(filename)
            except: 
                remote_size = -1

            if os.path.exists(local_path):
                local_size = os.path.getsize(local_path)
                if remote_size != -1 and local_size == remote_size:
                    logger.info(f"ℹ️ Arquivo {filename} já existe e está íntegro. Pulando.")
                    return local_path

            # 4. Download
            logger.info(f"⬇️ Baixando {filename}...")
            with open(local_path, 'wb') as f:
                self.ftp.retrbinary(f"RETR {filename}", f.write)
            
            logger.info(f"✅ Download concluído: {local_path}")
            return local_path

        except ftplib.error_perm as e:
            if "550" in str(e):
                logger.warning(f"⏳ Arquivo não encontrado (Erro 550).")
                return None
            else:
                logger.error(f"❌ Erro de permissão FTP: {e}")
                raise e
        except Exception as e:
            logger.error(f"❌ Erro genérico no download: {e}")
            raise e