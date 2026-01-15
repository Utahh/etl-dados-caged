import ftplib
import os
import logging
# Removida a importação do tqdm para evitar erro de dependência

# Configuração de Logger
logger = logging.getLogger(__name__)

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
            self.ftp.encoding = 'latin-1' # Importante para acentos
            return True
        except Exception as e:
            logger.error(f"❌ Erro ao conectar no FTP: {e}")
            raise e

    def close(self):
        if self.ftp:
            try: self.ftp.quit()
            except: pass

    def download_file(self, remote_path, filename, local_dir):
        """
        Retorna:
            - O caminho do arquivo (str) se baixou ou já existia.
            - None se o arquivo NÃO existir no servidor (Governo atrasou).
        """
        local_path = os.path.join(local_dir, filename)
        
        try:
            self.ftp.cwd(remote_path)
            
            # --- GATILHO DE SEGURANÇA 1: O arquivo existe? ---
            try:
                files_on_server = self.ftp.nlst()
            except ftplib.error_perm:
                # Tenta modo passivo ou fallback se nlst falhar
                files_on_server = []

            # Se conseguimos listar e o arquivo não está lá
            if files_on_server and filename not in files_on_server:
                logger.warning(f"⏳ AVISO: O arquivo '{filename}' AINDA NÃO EXISTE no servidor.")
                return None
            # -------------------------------------------------

            # Verifica tamanho (se já baixamos antes)
            try:
                remote_size = self.ftp.size(filename)
            except: 
                remote_size = -1

            if os.path.exists(local_path):
                local_size = os.path.getsize(local_path)
                if remote_size != -1 and local_size == remote_size:
                    logger.info(f"ℹ️ Arquivo {filename} já existe e está íntegro. Pulando download.")
                    return local_path

            # Download Simples (Sem barra de progresso para não sujar log do Airflow)
            logger.info(f"⬇️ Baixando {filename} (Tamanho: {remote_size/1024/1024:.2f} MB)...")
            
            with open(local_path, 'wb') as f:
                self.ftp.retrbinary(f"RETR {filename}", f.write)
            
            logger.info(f"✅ Download concluído: {local_path}")
            return local_path

        except ftplib.error_perm as e:
            # Erro 550 = File not found
            if "550" in str(e):
                logger.warning(f"⏳ Arquivo {filename} não encontrado (Erro 550). Governo ainda não publicou?")
                return None
            else:
                logger.error(f"❌ Erro de permissão no FTP: {e}")
                raise e
        except Exception as e:
            logger.error(f"❌ Erro genérico no download: {e}")
            raise e