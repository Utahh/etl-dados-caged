import os
import polars as pl
import py7zr
import logging
import unicodedata
import shutil

logger = logging.getLogger(__name__)

try:
    from .config import (RAW_EXTRACT_DIR, PROCESSED_DIR, COLUMNS_MAP, UF_FILTER, REFS_DIR, MAX_HISTORY_FILES)
except ImportError:
    from src.config import (RAW_EXTRACT_DIR, PROCESSED_DIR, COLUMNS_MAP, UF_FILTER, REFS_DIR, MAX_HISTORY_FILES)

class CagedProcessor:
    def __init__(self):
        os.makedirs(RAW_EXTRACT_DIR, exist_ok=True)
        os.makedirs(PROCESSED_DIR, exist_ok=True)

    def normalize_text(self, text: str) -> str:
        if not isinstance(text, str): return str(text)
        return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8').lower().strip()

    def cleanup_old_files(self):
        """Mantém apenas os 3 arquivos mais recentes na pasta processed"""
        try:
            files = [os.path.join(PROCESSED_DIR, f) for f in os.listdir(PROCESSED_DIR) if f.endswith('.csv') and 'SAMPLE' not in f]
            files.sort(key=os.path.getmtime, reverse=True) # Mais novos primeiro
            
            if len(files) > MAX_HISTORY_FILES:
                for old_file in files[MAX_HISTORY_FILES:]:
                    os.remove(old_file)
                    logger.info(f"🧹 Limpeza: Removido arquivo antigo {os.path.basename(old_file)}")
        except Exception as e:
            logger.warning(f"⚠️ Erro na limpeza de arquivos processados: {e}")

    def extract_file(self, zip_path: str) -> str:
        if not zip_path or not os.path.exists(zip_path): return None
        logger.info(f"📦 Descompactando: {os.path.basename(zip_path)}...")
        
        # Limpeza preventiva da pasta de extração
        for filename in os.listdir(RAW_EXTRACT_DIR):
            file_path = os.path.join(RAW_EXTRACT_DIR, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path): os.unlink(file_path)
                elif os.path.isdir(file_path): shutil.rmtree(file_path)
            except: pass

        try:
            with py7zr.SevenZipFile(zip_path, mode='r') as z:
                z.extractall(path=RAW_EXTRACT_DIR)
        except Exception as e:
            logger.error(f"❌ ARQUIVO CORROMPIDO: {e}")
            logger.warning(f"🗑️ Deletando {zip_path} para baixar novamente.")
            try: os.remove(zip_path)
            except: pass
            raise Exception("Arquivo zip corrompido e deletado.")

        for root, dirs, files in os.walk(RAW_EXTRACT_DIR):
            for f in files:
                if f.lower().endswith('.txt') or f.lower().endswith('.csv'):
                    return os.path.join(root, f)
        return None

    def convert_to_utf8(self, input_path: str) -> str:
        output_path = input_path + ".utf8"
        logger.info("🔄 Convertendo encoding (Latin-1 -> UTF-8)...")
        try:
            with open(input_path, 'r', encoding='latin-1', errors='replace') as s:
                with open(output_path, 'w', encoding='utf-8') as t:
                    while True:
                        chunk = s.read(1024*1024)
                        if not chunk: break
                        t.write(chunk)
            return output_path
        except: return input_path

    def process_data(self, txt_path: str, output_filename: str, year: str, month: str):
        utf8_path = None
        try:
            utf8_path = self.convert_to_utf8(txt_path)
            logger.info("🚀 Iniciando processamento (Polars)...")
            
            q = pl.scan_csv(utf8_path, separator=';', encoding='utf8', infer_schema_length=0, ignore_errors=True)
            
            # --- MAPEAMENTO ---
            file_cols_norm = {self.normalize_text(c): c for c in q.columns}
            rename_dict = {}
            
            for k, v in COLUMNS_MAP.items():
                if v in rename_dict.values(): continue
                norm = self.normalize_text(k)
                
                # Tentativa 1: Match Exato
                if norm in file_cols_norm:
                    rename_dict[file_cols_norm[norm]] = v
                else:
                    # Tentativa 2: Match Aproximado (StartsWith)
                    for f_norm, f_real in file_cols_norm.items():
                        if f_norm.startswith(norm):
                            rename_dict[f_real] = v
                            break
            
            q = q.select(list(rename_dict.keys())).rename(rename_dict)
            existing = rename_dict.values()

            # --- TIPAGEM ---
            for c in ['uf_codigo', 'municipio_codigo', 'saldo_movimentacao']:
                if c in existing: q = q.with_columns(pl.col(c).cast(pl.Int64, strict=False))
            
            if 'salario' in existing:
                q = q.with_columns(pl.col('salario').str.replace(',', '.').cast(pl.Float64, strict=False))

            if UF_FILTER and 'uf_codigo' in existing:
                q = q.filter(pl.col('uf_codigo') == UF_FILTER)
            
            q = q.with_columns(pl.lit(f"{year}-{month}-01").alias('competencia_declarada'))

            # --- ENRIQUECIMENTO (TABELA MÃE) ---
            # Verifica se mapeou corretamente 'secao_descricao'
            if 'secao_nome' in existing:
                ref_path = os.path.join(REFS_DIR, 'secoes.csv')
                if os.path.exists(ref_path):
                    logger.info("🔗 Cruzando com Tabela Mãe (Setores)...")
                    df_secoes = pl.scan_csv(ref_path, separator=';')
                    
                    q = q.with_columns(
                        pl.col('secao_nome')
                        .str.to_lowercase()
                        .str.replace_all(r"[^a-z0-9]", "") 
                        .alias('match_key')
                    )
                    
                    q = q.join(df_secoes, on='match_key', how='left')
                    q = q.with_columns(pl.col('atividade_economica').fill_null("Outros"))
                    q = q.drop(['match_key', 'secao_nome'])
            else:
                logger.warning("⚠️ Coluna 'secao_descricao' não encontrada. Verifique COLUMNS_MAP no config.py.")

            # --- SALVAR ARQUIVO FINAL ---
            output_path = os.path.join(PROCESSED_DIR, output_filename)
            logger.info(f"💾 Salvando {output_filename}...")
            
            df_final = q.collect()
            df_final.write_csv(output_path, separator=';')
            
            # --- NOVO: GERAR SAMPLE (TEMPLATE) ---
            sample_path = os.path.join(PROCESSED_DIR, "_SAMPLE_VERIFICACAO.csv")
            logger.info("🔬 Gerando arquivo de amostra (_SAMPLE_VERIFICACAO.csv)...")
            df_final.head(100).write_csv(sample_path, separator=';')
            
            # --- NOVO: LIMPEZA ---
            self.cleanup_old_files()

            logger.info(f"✅ Processamento OK: {df_final.height} linhas.")
            return output_path
            
        finally:
            if utf8_path and os.path.exists(utf8_path):
                try: os.remove(utf8_path)
                except: pass