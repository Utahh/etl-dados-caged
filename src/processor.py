import polars as pl
import os
import logging
import unicodedata
import re
from datetime import datetime, date

try:
    from .config import COLUMNS_MAP, PROCESSED_DIR
except ImportError:
    from src.config import COLUMNS_MAP, PROCESSED_DIR

logger = logging.getLogger(__name__)

def normalizar_texto(texto):
    if not texto: return ""
    texto = str(texto).lower().strip()
    texto = unicodedata.normalize('NFKD', texto)
    texto_sem_acento = "".join([c for c in texto if not unicodedata.combining(c)])
    texto_limpo = re.sub(r'[^a-z0-9_]', '', texto_sem_acento.replace(' ', '_').replace('-', '_'))
    return texto_limpo

class CagedProcessor:
    def __init__(self):
        self.column_mapping = COLUMNS_MAP
        self.sample_rows = []

    def process_data(self, txt_path, csv_filename, year, month, file_type):
        logger.info(f"🔨 Processando {file_type}...")
        try:
            # 1. Leitura
            df = pl.read_csv(txt_path, separator=';', encoding='iso-8859-1', infer_schema_length=0)
            
            # 2. Limpeza de Cabeçalho (Normalização)
            mapa_limpeza = {c: normalizar_texto(c) for c in df.columns} 
            df = df.rename(mapa_limpeza)

            # 3. Filtro SP (AQUI ESTAVA O ERRO)
            # O nome da coluna original é 'uf' (limpo), não 'uf_codigo' ainda.
            # Convertemos para int para garantir que bata com o número 35
            if "uf" in df.columns:
                df = df.filter(pl.col("uf").cast(pl.Int64, strict=False) == 35)
            elif "uf_codigo" in df.columns:
                df = df.filter(pl.col("uf_codigo").cast(pl.Int64, strict=False) == 35)
            
            # Se não achar nenhuma das duas, loga um aviso (mas não quebra, caso o layout mude)
            else:
                logger.warning(f"⚠️ Coluna de UF não encontrada em {file_type}. Colunas: {df.columns}")

            # 4. Renomeação Oficial (Mapping do Config)
            cols_to_rename = {k: v for k, v in self.column_mapping.items() if k in df.columns}
            df = df.rename(cols_to_rename)

            # 5. Tratamento de Tipos Básicos
            if "salario" in df.columns:
                df = df.with_columns(pl.col("salario").str.replace(",", ".").cast(pl.Float64, strict=False).fill_null(0.0))
            
            if "saldo_movimentacao" in df.columns:
                df = df.with_columns(pl.col("saldo_movimentacao").cast(pl.Int64, strict=False).fill_null(0))
                if file_type == "CAGEDEXC":
                    df = df.with_columns((pl.col("saldo_movimentacao") * -1).alias("saldo_movimentacao"))

            # --- 6. DATAS ---
            
            # A) data_ref (Baseado na competência)
            if "competencia_mov" in df.columns:
                df = df.with_columns(
                    pl.col("competencia_mov")
                    .cast(pl.Utf8)
                    .add("01")
                    .str.strptime(pl.Date, "%Y%m%d")
                    .alias("data_ref")
                )

            # B) data_arquivo
            data_arquivo_dt = date(int(year), int(month), 1)
            
            # C) data_processamento
            data_hoje_dt = datetime.now()

            df = df.with_columns([
                pl.lit(data_arquivo_dt).alias("data_arquivo"),
                pl.lit(data_hoje_dt).alias("data_processamento"),
                pl.lit(file_type).alias("tipo_arquivo")
            ])
            # ----------------

            self.sample_rows.append(df.head(2))
            output_path = os.path.join(PROCESSED_DIR, csv_filename)
            df.write_csv(output_path, separator=';')
            return output_path

        except Exception as e:
            logger.error(f"❌ Erro Processor: {e}"); raise e

    # ... Resto do código (generate_sample_file e extract_file) igual ...
    def generate_sample_file(self):
        if self.sample_rows:
            sample_df = pl.concat(self.sample_rows, how="diagonal")
            sample_path = os.path.join(PROCESSED_DIR, "tabela_geral_sample.csv")
            sample_df.write_csv(sample_path, separator=';')

    def extract_file(self, zip_path):
        import py7zr
        output_dir = os.path.dirname(zip_path)
        with py7zr.SevenZipFile(zip_path, mode='r') as z:
            z.extractall(path=output_dir)
            for f in z.getnames():
                if f.lower().endswith(('.txt', '.csv')): return os.path.join(output_dir, f)
        return None