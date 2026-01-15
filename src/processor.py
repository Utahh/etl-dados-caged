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
        try:
            files = [os.path.join(PROCESSED_DIR, f) for f in os.listdir(PROCESSED_DIR) if f.endswith('.csv') and 'SAMPLE' not in f]
            files.sort(key=os.path.getmtime, reverse=True)
            if len(files) > MAX_HISTORY_FILES:
                for old_file in files[MAX_HISTORY_FILES:]:
                    try: os.remove(old_file)
                    except: pass
        except: pass

    def extract_file(self, zip_path: str) -> str:
        if not zip_path or not os.path.exists(zip_path): return None
        logger.info(f"📦 Descompactando: {os.path.basename(zip_path)}...")
        
        for f in os.listdir(RAW_EXTRACT_DIR):
            try: 
                path = os.path.join(RAW_EXTRACT_DIR, f)
                if os.path.isdir(path): shutil.rmtree(path)
                else: os.unlink(path)
            except: pass

        try:
            with py7zr.SevenZipFile(zip_path, mode='r') as z:
                z.extractall(path=RAW_EXTRACT_DIR)
        except Exception as e:
            logger.error(f"❌ Zip Corrompido: {e}")
            try: os.remove(zip_path)
            except: pass
            raise Exception("Arquivo zip corrompido.")

        for root, dirs, files in os.walk(RAW_EXTRACT_DIR):
            for f in files:
                if f.lower().endswith(('.txt', '.csv')): return os.path.join(root, f)
        return None

    def convert_to_utf8(self, input_path: str) -> str:
        out = input_path + ".utf8"
        logger.info("🔄 Convertendo encoding...")
        try:
            with open(input_path, 'r', encoding='latin-1', errors='replace') as s:
                with open(out, 'w', encoding='utf-8') as t:
                    while True:
                        c = s.read(1024*1024)
                        if not c: break
                        t.write(c)
            return out
        except: return input_path

    def process_data(self, txt_path: str, output_filename: str, year: str, month: str):
        utf8_path = None
        try:
            utf8_path = self.convert_to_utf8(txt_path)
            logger.info("🚀 Processando (Polars)...")
            
            # 1. Scan Lazy
            q = pl.scan_csv(utf8_path, separator=';', encoding='utf8', infer_schema_length=0, ignore_errors=True)
            
            # ==================================================================
            # 🛡️ MAPEAMENTO INTELIGENTE (SEM DUPLICATAS)
            # ==================================================================
            # Cria um dicionário das colunas do arquivo normalizadas
            file_cols_norm = {self.normalize_text(c): c for c in q.columns}
            
            # Lista de expressões de seleção (SELECT ... AS ...)
            selection_exprs = []
            used_targets = set()

            for key_map, target_name in COLUMNS_MAP.items():
                if target_name in used_targets: 
                    continue # Já pegamos uma coluna para esse campo (Ex: salario), ignora as próximas
                
                key_norm = self.normalize_text(key_map)
                
                # Procura Match Exato
                if key_norm in file_cols_norm:
                    real_col_name = file_cols_norm[key_norm]
                    selection_exprs.append(pl.col(real_col_name).alias(target_name))
                    used_targets.add(target_name)
                else:
                    # Procura Match Aproximado (StartsWith)
                    for f_norm, f_real in file_cols_norm.items():
                        if f_norm.startswith(key_norm):
                            selection_exprs.append(pl.col(f_real).alias(target_name))
                            used_targets.add(target_name)
                            break
            
            # Aplica a seleção (Isso remove colunas inúteis e renomeia as úteis de uma vez)
            q = q.select(selection_exprs)
            existing = used_targets # Conjunto dos nomes finais (ex: 'salario', 'subclasse_codigo')

            # ==================================================================
            # 🔧 TIPAGEM
            # ==================================================================
            for c in ['uf_codigo', 'municipio_codigo', 'saldo_movimentacao', 'subclasse_codigo']:
                if c in existing: q = q.with_columns(pl.col(c).cast(pl.Int64, strict=False))
            
            if 'salario' in existing:
                # Remove vírgula, converte pra float
                q = q.with_columns(pl.col('salario').str.replace(',', '.').cast(pl.Float64, strict=False))

            if UF_FILTER and 'uf_codigo' in existing:
                q = q.filter(pl.col('uf_codigo') == UF_FILTER)
            
            q = q.with_columns(pl.lit(f"{year}-{month}-01").alias('competencia_declarada'))

            # ==================================================================
            # 🌟 ENRIQUECIMENTO (JOIN via CÓDIGOS)
            # ==================================================================
            cnae_path = os.path.join(REFS_DIR, 'cnae.csv')
            secoes_path = os.path.join(REFS_DIR, 'secoes.csv')

            if 'subclasse_codigo' in existing and os.path.exists(cnae_path) and os.path.exists(secoes_path):
                logger.info("🔗 Join 1: Subclasse -> CNAE (Pegando Código da Seção)")
                df_cnae = pl.scan_csv(cnae_path, separator=';')
                q = q.join(df_cnae, on='subclasse_codigo', how='left')
                
                logger.info("🔗 Join 2: Código da Seção -> Tabela Mãe")
                df_secoes = pl.scan_csv(secoes_path, separator=';')
                q = q.join(df_secoes, on='secao_codigo', how='left')
            
            # Preenche buracos
            cols_check = q.columns
            q = q.with_columns([
                pl.col('secao_descricao').fill_null('Não Identificado') if 'secao_descricao' in cols_check else pl.lit('NR').alias('secao_descricao'),
                pl.col('atividade_economica').fill_null('Outros') if 'atividade_economica' in cols_check else pl.lit('Outros').alias('atividade_economica')
            ])

            # --- Salvar ---
            out_path = os.path.join(PROCESSED_DIR, output_filename)
            logger.info(f"💾 Salvando CSV...")
            
            df_final = q.collect()
            df_final.write_csv(out_path, separator=';')
            
            # Sample
            sample_path = os.path.join(PROCESSED_DIR, "_SAMPLE_VERIFICACAO.csv")
            df_final.head(100).write_csv(sample_path, separator=';')
            
            self.cleanup_old_files()
            logger.info(f"✅ Sucesso: {df_final.height} linhas.")
            return out_path
            
        finally:
            if utf8_path and os.path.exists(utf8_path):
                try: os.remove(utf8_path)
                except: pass