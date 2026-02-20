import polars as pl
import os
import logging
import unicodedata
import re
import subprocess
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

    def extract_file(self, zip_path):
        """Extrai o arquivo .7z de forma robusta."""
        try:
            folder = os.path.dirname(zip_path)
            cmd = ["7z", "e", zip_path, f"-o{folder}", "-y"]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode != 0:
                logger.error(f"❌ Erro no 7z: {result.stderr}")
                return None
            
            files_in_folder = os.listdir(folder)
            extracted_files = [f for f in files_in_folder if f.lower().endswith(('.txt', '.csv'))]
            
            if not extracted_files:
                return None
            
            return os.path.join(folder, extracted_files[0])
            
        except Exception as e:
            logger.error(f"❌ Erro crítico extração: {e}")
            return None

    def process_data(self, txt_path, csv_filename, year, month, file_type):
        logger.info(f"🔨 Processando {file_type}...")
        try:
            # 1. Leitura
            df = pl.read_csv(
                txt_path, 
                separator=';', 
                encoding='utf8-lossy', 
                infer_schema_length=0, 
                ignore_errors=True
            )
            
            # 2. Normalização de Nomes
            novo_mapa = {}
            for col in df.columns:
                col_limpa = normalizar_texto(col)
                nome_final = self.column_mapping.get(col_limpa, col_limpa)
                novo_mapa[col] = nome_final

            df = df.rename(novo_mapa)

            # 3. Filtro SP
            if "uf_codigo" in df.columns:
                df = df.filter(pl.col("uf_codigo").cast(pl.Int64, strict=False) == 35)
            elif "uf" in df.columns:
                df = df.filter(pl.col("uf").cast(pl.Int64, strict=False) == 35)

            # 4. Tratamento de Moeda
            cols_moeda = ["salario", "valor_salario_fixo"]
            for c in cols_moeda:
                if c in df.columns:
                    df = df.with_columns(
                        pl.col(c)
                        .str.replace(r"R\$", "")
                        .str.replace(r"\.", "")
                        .str.replace(",", ".")
                        .cast(pl.Float64, strict=False)
                        .fill_null(0.0)
                    )

            # 5. Tratamento de Exclusão
            if file_type == "CAGEDEXC" and "saldo_movimentacao" in df.columns:
                df = df.with_columns(
                    (pl.col("saldo_movimentacao").cast(pl.Int64, strict=False) * -1)
                )

            # 6. Converte competência para Data Real
            if "competencia_mov" in df.columns:
                df = df.with_columns(
                    pl.col("competencia_mov")
                    .cast(pl.Utf8)       
                    .add("01")           
                    .str.strptime(pl.Date, "%Y%m%d", strict=False) 
                    .alias("competencia_mov") 
                )

            # 7. Metadados
            data_ref_iso = f"{year}-{month}-01"
            df = df.with_columns(pl.lit(data_ref_iso).alias("data_ref"))
            df = df.with_columns(pl.col("data_ref").cast(pl.Date))

            df = df.with_columns([
                pl.lit(date(int(year), int(month), 1)).alias("data_arquivo"),
                pl.lit(datetime.now()).alias("data_processamento"),
                pl.lit(file_type).alias("tipo_arquivo")
            ])

            # 8. Salva
            output_path = os.path.join(PROCESSED_DIR, csv_filename)
            df.write_csv(output_path, separator=';', datetime_format="%Y-%m-%d")
            return output_path

        except Exception as e:
            logger.error(f"❌ Erro Processor: {e}"); raise e