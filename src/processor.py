import os
import polars as pl
import py7zr
import logging
import unicodedata

logger = logging.getLogger(__name__)

try:
    from .config import (RAW_EXTRACT_DIR, PROCESSED_DIR, COLUMNS_MAP, UF_FILTER, REFS_DIR)
except ImportError:
    from src.config import (RAW_EXTRACT_DIR, PROCESSED_DIR, COLUMNS_MAP, UF_FILTER, REFS_DIR)

class CagedProcessor:
    def __init__(self):
        os.makedirs(RAW_EXTRACT_DIR, exist_ok=True)
        os.makedirs(PROCESSED_DIR, exist_ok=True)

    def normalize_text(self, text: str) -> str:
        if not isinstance(text, str): return str(text)
        text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
        return text.lower().strip().replace(' ', '_').replace('.', '').replace('-', '_')

    def extract_file(self, zip_path: str) -> str:
        if not zip_path or not os.path.exists(zip_path): return None
        logger.info("📦 Descompactando ficheiro...")
        try:
            for f in os.listdir(RAW_EXTRACT_DIR):
                try: os.remove(os.path.join(RAW_EXTRACT_DIR, f))
                except: pass
            
            with py7zr.SevenZipFile(zip_path, mode='r') as z:
                z.extractall(path=RAW_EXTRACT_DIR)
            
            extracted = [f for f in os.listdir(RAW_EXTRACT_DIR) if f.endswith('.txt') or f.endswith('.csv')]
            return os.path.join(RAW_EXTRACT_DIR, extracted[0]) if extracted else None
        except Exception as e:
            logger.error(f"❌ Erro na extração: {e}")
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
        except Exception as e:
            logger.error(f"❌ Erro encoding: {e}")
            raise e

    def load_ref_table(self, filename: str, expected_key: str):
        path = os.path.join(REFS_DIR, filename)
        if not os.path.exists(path): return None
        try:
            df = pl.read_csv(path, separator=';', infer_schema_length=0)
            df = df.rename({c: self.normalize_text(c) for c in df.columns})
            key_norm = self.normalize_text(expected_key)
            final_key = expected_key if expected_key in df.columns else (key_norm if key_norm in df.columns else None)
            
            if not final_key: return None
            return df.with_columns(pl.col(final_key).cast(pl.Int64, strict=False))
        except: return None

    def process_data(self, txt_path: str, output_filename: str, year: str, month: str):
        utf8_path = None
        try:
            utf8_path = self.convert_to_utf8(txt_path)
            logger.info("🚀 Iniciando processamento (ETL na Memória)...")
            
            q = pl.scan_csv(utf8_path, separator=';', encoding='utf8', infer_schema_length=0, ignore_errors=True)
            
            file_cols_norm = {self.normalize_text(c): c for c in q.columns}
            rename_dict = {}
            
            # --- CORREÇÃO: LÓGICA DE 2 PASSOS ---
            
            # Passo 1: Matches Exatos (Prioridade Máxima)
            for k, v in COLUMNS_MAP.items():
                if v in rename_dict.values(): continue
                norm = self.normalize_text(k)
                if norm in file_cols_norm:
                    rename_dict[file_cols_norm[norm]] = v
            
            # Passo 2: Matches Aproximados (Fuzzy) - Apenas para o que sobrou
            for k, v in COLUMNS_MAP.items():
                if v in rename_dict.values(): continue # Se já achou no passo 1, pula
                
                norm = self.normalize_text(k)
                for f_norm, f_real in file_cols_norm.items():
                    if f_norm.startswith(norm):
                        rename_dict[f_real] = v
                        break
            # ------------------------------------

            logger.info(f"📋 Colunas mapeadas: {len(rename_dict)} de {len(COLUMNS_MAP)} (Esperado: ~20)")
            
            # Verifica se colunas cruciais foram encontradas
            if 'municipio_codigo' not in rename_dict.values():
                logger.warning("⚠️ ALERTA: Coluna 'municipio_codigo' NÃO foi encontrada no arquivo!")
                logger.warning(f"   Colunas disponíveis no arquivo: {list(file_cols_norm.keys())}")
            
            q = q.select(list(rename_dict.keys())).rename(rename_dict)
            
            existing = rename_dict.values()
            
            for c in ['uf_codigo', 'municipio_codigo', 'subclasse_codigo', 'saldo_movimentacao', 'secao_codigo']:
                if c in existing: q = q.with_columns(pl.col(c).cast(pl.Int64, strict=False))
                
            if 'salario' in existing:
                q = q.with_columns(pl.col('salario').str.replace(',', '.').cast(pl.Float64, strict=False))
                
            if UF_FILTER and 'uf_codigo' in existing:
                q = q.filter(pl.col('uf_codigo') == UF_FILTER)
                
            q = q.with_columns(pl.lit(f"{year}-{month}-01").alias('competencia_declarada'))
            
            logger.info("🔄 Cruzando dados com tabelas de referência...")
            if 'municipio_codigo' in existing:
                df_mun = self.load_ref_table('municipios.csv', 'municipio_codigo')
                if df_mun is not None: q = q.join(df_mun.lazy(), on='municipio_codigo', how='left')
            
            if 'subclasse_codigo' in existing:
                df_cnae = self.load_ref_table('cnae.csv', 'subclasse_codigo')
                if df_cnae is not None: q = q.join(df_cnae.lazy(), on='subclasse_codigo', how='left')

            output_path = os.path.join(PROCESSED_DIR, output_filename)
            logger.info("💾 Coletando dados para memória...")
            df_final = q.collect()
            logger.info(f"💾 Salvando CSV final com {df_final.height} linhas...")
            df_final.write_csv(output_path, separator=';')
            logger.info(f"✅ Arquivo gerado com sucesso: {output_path}")
            return output_path
            
        finally:
            if utf8_path and os.path.exists(utf8_path):
                try: os.remove(utf8_path)
                except: pass