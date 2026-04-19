import polars as pl
import os
import logging
import unicodedata
import re
import py7zr  # Substitui o subprocess para extração nativa
from datetime import datetime, date

# --- CORREÇÃO DE IMPORTAÇÃO (ARQUITETURA LAKEHOUSE) ---
from src.core.config import COLUMNS_MAP, PROCESSED_DIR

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
        """Extrai o arquivo .7z de forma robusta e 100% Python."""
        logger.info(f"📦 Iniciando extração nativa de: {zip_path}")
        try:
            folder = os.path.dirname(zip_path)
            
            # Antecipação: Garante que a pasta de destino existe
            os.makedirs(folder, exist_ok=True)
            
            extracted_filename = None
            
            # mode='r' lê o arquivo usando py7zr
            with py7zr.SevenZipFile(zip_path, mode='r') as archive:
                # LÓGICA ESPECIALISTA: Descobre os nomes dos arquivos DENTRO do 7z
                # Isso evita ler arquivos residuais de execuções anteriores no mesmo diretório
                all_files_in_archive = archive.getnames()
                target_files = [f for f in all_files_in_archive if f.lower().endswith(('.txt', '.csv'))]
                
                if not target_files:
                    logger.error(f"❌ Nenhum arquivo .txt ou .csv válido dentro de {zip_path}")
                    return None
                
                extracted_filename = target_files[0]
                
                # Extrai fisicamente para a pasta
                archive.extractall(path=folder)
            
            final_path = os.path.join(folder, extracted_filename)
            logger.info(f"✅ Extração concluída: {final_path}")
            return final_path
            
        except py7zr.exceptions.Bad7zFile:
            logger.error(f"❌ Erro Crítico: O arquivo {zip_path} está corrompido ou não é um .7z válido (Falha no FTP?).")
            return None
        except MemoryError:
            logger.error(f"❌ Erro de Memória (OOM): O Airflow Worker não tem RAM suficiente para extrair {zip_path}.")
            return None
        except Exception as e:
            logger.error(f"❌ Erro crítico extração py7zr: {e}")
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