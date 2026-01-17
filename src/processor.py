import polars as pl
import os
import logging
from datetime import date

try:
    from .config import REFS_FILES, COLUMNS_MAP, PROCESSED_DIR
except ImportError:
    from src.config import REFS_FILES, COLUMNS_MAP, PROCESSED_DIR

logger = logging.getLogger(__name__)

class CagedProcessor:
    def __init__(self):
        self.column_mapping = COLUMNS_MAP

    def extract_file(self, zip_path):
        try:
            import py7zr
            output_dir = os.path.dirname(zip_path)
            logger.info(f"📦 Extraindo {zip_path}...")
            with py7zr.SevenZipFile(zip_path, mode='r') as z:
                z.extractall(path=output_dir)
                for f in z.getnames():
                    if f.lower().endswith(('.txt', '.csv')):
                        return os.path.join(output_dir, f)
            return None
        except Exception as e:
            logger.error(f"❌ Erro na extração: {e}")
            return None

    def load_ref(self, key_name, col_cod, col_desc):
        """Carrega CSV de referência e normaliza chaves."""
        path = REFS_FILES.get(key_name)
        
        if path and not os.path.exists(path):
            alt = path.replace("municipios", "municípios").replace("secoes", "seções")
            if os.path.exists(alt): path = alt
            
        if not path or not os.path.exists(path):
            logger.warning(f"⚠️ Ref '{key_name}' não encontrada.")
            return None

        try:
            df = pl.read_csv(path, separator=';', infer_schema_length=0)
            df.columns = [c.lower().strip() for c in df.columns]
            
            # Busca dinâmica: Verifica se o trecho (ex: 'codigo') existe no nome da coluna
            c_join = next((c for c in df.columns if col_cod in c), None)
            c_desc = next((c for c in df.columns if col_desc in c), None)

            if not c_join or not c_desc: 
                logger.warning(f"⚠️ Colunas não encontradas em {key_name}. Procurando: {col_cod}, {col_desc}. Tem: {df.columns}")
                return None
            
            df = df.select([
                pl.col(c_join).cast(pl.Utf8).alias("ref_key"),
                pl.col(c_desc).alias(f"{key_name}_desc")
            ])

            if key_name == "municipios":
                df = df.with_columns(pl.col("ref_key").str.slice(0, 6))

            return df
        except Exception as e:
            logger.error(f"❌ Erro lendo {key_name}: {e}")
            return None

    def process_data(self, txt_path, csv_filename, year, month):
        logger.info(f"🔨 Processando: {txt_path}")
        try:
            # 1. LEITURA
            df = pl.scan_csv(txt_path, separator=';', encoding='utf8-lossy', infer_schema_length=0, null_values=['NA','nan','null',''])
            df = df.rename({c: c.lower().strip() for c in df.columns})
            
            if "uf" in df.columns: 
                df = df.filter(pl.col("uf") == "35")

            cols_to_rename = {k: v for k, v in self.column_mapping.items() if k in df.columns}
            df = df.rename(cols_to_rename)
            
            df_final = df.collect()

            # --- CHECKPOINT 1: AUDITORIA SP BRUTO ---
            count_sp_raw = df_final.height
            logger.info(f"📊 [AUDITORIA] Qtd Registros SP (Antes do Tratamento): {count_sp_raw}")

            # 2. ENRIQUECIMENTO (JOINS)
            
            # A) MUNICÍPIO
            ref_muni = self.load_ref("municipios", "codigo", "nome")
            if ref_muni is not None and "municipio_codigo" in df_final.columns:
                df_final = df_final.with_columns(pl.col("municipio_codigo").str.slice(0, 6).alias("join_muni"))
                df_final = df_final.join(ref_muni, left_on="join_muni", right_on="ref_key", how="left")
                df_final = df_final.rename({"municipios_desc": "municipio_nome"})

            # B) SEÇÃO (CORREÇÃO AQUI) -> Mudamos de "desc" para "atividade"
            # O arquivo tem 'atividade_economica', então 'desc' não encontrava nada.
            ref_secao = self.load_ref("secoes", "codigo", "atividade")
            if ref_secao is not None:
                df_final = df_final.join(ref_secao, left_on="secao_codigo", right_on="ref_key", how="left")
                df_final = df_final.rename({"secoes_desc": "secao_nome"})

            # C) SUBCLASSE
            ref_sub = self.load_ref("subclasse", "subclasse", "desc")
            if ref_sub is not None:
                df_final = df_final.join(ref_sub, left_on="subclasse_codigo", right_on="ref_key", how="left")
                df_final = df_final.rename({"subclasse_desc": "subclasse_descricao"})

            # D) DEMAIS JOINS
            joins = [
                ("categoria", "categoria_codigo", "categoria_desc", "categoria_desc", "codigo", "desc"),
                ("grau_instrucao", "grau_instrucao_codigo", "grau_instrucao_desc", "grau_instrucao_desc", "codigo", "desc"),
                ("tipo_movimentacao", "tipo_movimentacao_codigo", "tipo_movimentacao_desc", "tipo_movimentacao_desc", "codigo", "desc")
            ]
            
            for key, col_fk, col_orig_desc, col_final_desc, ref_k, ref_v in joins:
                ref = self.load_ref(key, ref_k, ref_v)
                if ref is not None:
                    df_final = df_final.join(ref, left_on=col_fk, right_on="ref_key", how="left")
                    df_final = df_final.rename({f"{key}_desc": col_final_desc})

            # Mapas Manuais
            sexo_map = pl.DataFrame({"k": ["1", "3"], "v": ["Masculino", "Feminino"]})
            df_final = df_final.join(sexo_map, left_on="sexo_codigo", right_on="k", how="left").rename({"v": "sexo_descricao"})
            raca_map = pl.DataFrame({"k": ["1","2","4","6","8","9"], "v": ["Branca","Preta","Parda","Amarela","Indígena","Ignorado"]})
            df_final = df_final.join(raca_map, left_on="raca_cor_codigo", right_on="k", how="left").rename({"v": "raca_cor_desc"})

            # 3. FINALIZAÇÃO
            ref_date = f"{year}-{month}-01"
            proc_date = date.today().isoformat()
            df_final = df_final.with_columns([
                pl.lit(ref_date).alias("data_ref"), pl.lit(proc_date).alias("data_proc"), pl.lit("SP").alias("uf_sigla")
            ])

            if "salario" in df_final.columns:
                df_final = df_final.with_columns(pl.col("salario").str.replace(",", ".").cast(pl.Float64, strict=False).fill_null(0.0))

            desc_cols = ["municipio_nome", "secao_nome", "subclasse_descricao", "categoria_desc", "grau_instrucao_desc", "tipo_movimentacao_desc", "sexo_descricao", "raca_cor_desc"]
            for c in desc_cols:
                if c not in df_final.columns: df_final = df_final.with_columns(pl.lit("Não Identificado").alias(c))
                else: df_final = df_final.with_columns(pl.col(c).fill_null("Não Identificado"))

            final_cols = ["uf_codigo", "municipio_codigo", "secao_codigo", "subclasse_codigo", "saldo_movimentacao", "categoria_codigo", "grau_instrucao_codigo", "idade", "raca_cor_codigo", "sexo_codigo", "tipo_movimentacao_codigo", "salario", "data_ref", "data_proc", "municipio_nome", "uf_sigla", "subclasse_descricao", "secao_nome", "grau_instrucao_desc", "categoria_desc", "tipo_movimentacao_desc", "sexo_descricao", "raca_cor_desc"]
            existing = [c for c in final_cols if c in df_final.columns]
            df_final = df_final.select(existing)

            # --- CHECKPOINT 2: AUDITORIA FINAL ---
            count_sp_final = df_final.height
            logger.info(f"📊 [AUDITORIA] Qtd Registros SP (Após Tratamento): {count_sp_final}")
            
            if count_sp_final != count_sp_raw:
                logger.warning(f"⚠️ Diferença de registros: {count_sp_final - count_sp_raw}")
            else:
                logger.info("✅ Integridade OK.")

            output_path = os.path.join(PROCESSED_DIR, csv_filename)
            df_final.write_csv(output_path, separator=';')
            return output_path

        except Exception as e:
            logger.error(f"❌ Erro crítico: {e}")
            raise e