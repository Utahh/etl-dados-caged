import polars as pl
import os
import logging
import unicodedata
import re
from datetime import date

try:
    from .config import COLUMNS_MAP, PROCESSED_DIR, REFS_FILES
except ImportError:
    from src.config import COLUMNS_MAP, PROCESSED_DIR, REFS_FILES

logger = logging.getLogger(__name__)

def normalizar_texto(texto):
    """
    Transforma: 'Município' -> 'municipio'
    Transforma: 'Competência' -> 'competencia'
    Transforma: 'Salário (R$)' -> 'salario_r'
    Remove tudo que não for letra ou número.
    """
    if not texto: return ""
    # 1. Converte para string e minúsculo
    texto = str(texto).lower().strip()
    
    # 2. Normalização Unicode (Separa o acento da letra: 'ç' vira 'c' + '¸')
    texto_normalizado = unicodedata.normalize('NFKD', texto)
    
    # 3. Mantém apenas caracteres ASCII (remove os acentos separados no passo anterior)
    texto_sem_acento = "".join([c for c in texto_normalizado if not unicodedata.combining(c)])
    
    # 4. Remove qualquer caractere que NÃO seja letra (a-z), número (0-9) ou underline
    # Substitui espaços e hifens por underline antes
    texto_limpo = re.sub(r'[^a-z0-9_]', '', texto_sem_acento.replace(' ', '_').replace('-', '_'))
    
    return texto_limpo

class CagedProcessor:
    def __init__(self):
        self.column_mapping = COLUMNS_MAP
        self.sample_rows = []

    def process_data(self, txt_path, csv_filename, year, month, file_type):
        logger.info(f"🔨 Processando {file_type}...")
        try:
            # 1. LEITURA
            # O 'infer_schema_length=0' lê tudo como string para evitar erros de tipo na leitura
            df = pl.read_csv(txt_path, separator=';', encoding='iso-8859-1', infer_schema_length=0)
            
            # 2. LIMPEZA TOTAL DE CABEÇALHOS (AQUI ESTÁ O SEGREDO)
            # Criamos um dicionário: {nome_velho_sujo: nome_novo_limpo}
            mapa_limpeza = {col: normalizar_texto(col) for col in df.columns}
            df = df.rename(mapa_limpeza)
            
            # --- DEBUG NO LOG ---
            # Isso vai te mostrar no Airflow como os nomes ficaram após a limpeza
            logger.info(f"🧹 Colunas limpas: {df.columns[:5]} ...") 

            # 3. FILTRO SP (Agora buscamos pelas colunas já limpas)
            # O normalizar_texto transformou 'uf' ou 'uf_codigo' em algo padrão
            if "uf_codigo" in df.columns:
                df = df.filter(pl.col("uf_codigo") == "35")
            elif "uf" in df.columns:
                df = df.filter(pl.col("uf") == "35")

            # 4. MAPEAMENTO DE NEGÓCIO
            # Agora aplicamos o mapa do config.py. 
            # IMPORTANTE: As chaves do seu config.py devem estar LIMPAS agora.
            cols_to_rename = {k: v for k, v in self.column_mapping.items() if k in df.columns}
            df = df.rename(cols_to_rename)

            # 5. TRATAMENTO NUMÉRICO
            if "salario" in df.columns:
                df = df.with_columns(pl.col("salario").str.replace(",", ".").cast(pl.Float64, strict=False).fill_null(0.0))
            
            if "saldo_movimentacao" in df.columns:
                df = df.with_columns(pl.col("saldo_movimentacao").cast(pl.Int64, strict=False).fill_null(0))
                if file_type == "CAGEDEXC":
                    df = df.with_columns((pl.col("saldo_movimentacao") * -1).alias("saldo_movimentacao"))

            # 6. JOINS (Sample)
            if os.path.exists(REFS_FILES['municipios']) and "municipio_codigo" in df.columns:
                muni_ref = pl.read_csv(REFS_FILES['municipios'], separator=';', infer_schema_length=0)
                muni_ref = muni_ref.rename({c: normalizar_texto(c) for c in muni_ref.columns})
                # Garante chaves limpas no ref também
                if "municipio" in muni_ref.columns: muni_ref = muni_ref.rename({"municipio": "municipio_codigo"})

                df = df.with_columns(pl.col("municipio_codigo").cast(pl.Utf8))
                muni_ref = muni_ref.with_columns(pl.col("municipio_codigo").cast(pl.Utf8))
                
                # Tenta encontrar a coluna de nome do município
                col_nome_muni = "municipio_nome" if "municipio_nome" in muni_ref.columns else "municipio_desc"
                if col_nome_muni in muni_ref.columns:
                    df = df.join(muni_ref.select(["municipio_codigo", col_nome_muni]), on="municipio_codigo", how="left")

            # 7. METADADOS
            df = df.with_columns([
                pl.lit(f"{year}-{month}-01").alias("data_ref_carga"), 
                pl.lit(file_type).alias("tipo_arquivo") 
            ])

            self.sample_rows.append(df.head(2))
            output_path = os.path.join(PROCESSED_DIR, csv_filename)
            df.write_csv(output_path, separator=';')
            return output_path

        except Exception as e:
            logger.error(f"❌ Erro Processor: {e}"); raise e

    # ... (restante dos métodos generate_sample_file e extract_file igual) ...
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