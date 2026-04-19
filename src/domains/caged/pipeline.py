import os
import glob
import logging
import polars as pl
from sqlalchemy import create_engine, text

# --- IMPORTS DA ARQUITETURA LAKEHOUSE ---
from src.core.config import DB_URL, RAW_ZIP_DIR, TABLE_NAME, PROCESSED_DIR, REFS_DIR
from src.core.ftp_client import FTPClient
from src.core.db_loader import load_to_database
from src.domains.caged.processor import CagedProcessor

logger = logging.getLogger(__name__)

# --- DICIONÁRIOS ESTÁTICOS (Padrão IBGE/CAGED) ---
MAP_SEXO = { 1: "Masculino", 3: "Feminino", 9: "Não Identificado" }

MAP_RACA = {
    1: "Branca", 2: "Preta", 3: "Parda", 
    4: "Amarela", 5: "Indígena", 6: "Não informada", 9: "Não identificado"
}

def create_index_pk(engine, table_name, pk_col):
    """Cria a Chave Primária (PK) garantindo a formatação exata do banco."""
    try:
        with engine.begin() as conn:
            conn.execute(text(f'ALTER TABLE "{table_name}" ALTER COLUMN "{pk_col}" SET NOT NULL;'))
            conn.execute(text(f'ALTER TABLE "{table_name}" ADD PRIMARY KEY ("{pk_col}");'))
        logger.info(f"🔑 Índice PK criado com sucesso em {table_name}.{pk_col}")
    except Exception as e:
        logger.warning(f"⚠️ Aviso ao criar PK em {table_name} (pode já existir ou ter duplicatas): {e}")

def create_static_dimension(db_url, table_name, data_dict, col_id, col_desc):
    """Cria tabelas pequenas (Sexo, Raça) usando Polars Puro e a URL do banco."""
    try:
        df_pl = pl.DataFrame({
            col_id: list(data_dict.keys()),
            col_desc: list(data_dict.values())
        })
        # Passamos a URL diretamente para o Polars gerenciar a conexão
        df_pl.write_database(table_name=table_name, connection=db_url, if_exists='replace')
        
        # O SQLAlchemy continua sendo usado apenas para executar os comandos de índice
        engine = create_engine(db_url)
        create_index_pk(engine, table_name, col_id)
        logger.info(f"✅ Tabela {table_name} criada via Polars.")
    except Exception as e:
        logger.error(f"❌ Erro ao criar {table_name}: {e}")

def create_csv_dimension_polars(db_url, table_name, csv_filename, col_mapping, pk_col_db):
    """Lê o arquivo CSV com Polars e grava no banco de dados usando a URL."""
    csv_path = os.path.join(REFS_DIR, csv_filename)
    if not os.path.exists(csv_path):
        logger.warning(f"⚠️ Arquivo {csv_filename} não encontrado em {REFS_DIR}. Pulando {table_name}.")
        return False

    try:
        df = pl.read_csv(
            csv_path, 
            separator=';', 
            infer_schema_length=0, 
            encoding='utf8-lossy',
            ignore_errors=True
        )
        
        cols_to_select = []
        for csv_col, db_col in col_mapping.items():
            if csv_col in df.columns:
                cols_to_select.append(pl.col(csv_col).alias(db_col))
        
        if not cols_to_select:
            logger.error(f"❌ Nenhuma coluna compatível encontrada em {csv_filename}.")
            return False

        df_final = df.select(cols_to_select).unique(subset=[pk_col_db], keep='first')

        logger.info(f"🚀 Inserindo {df_final.height} linhas em {table_name} (Polars)...")
        # Usando a DB_URL string para evitar conflitos com o SQLAlchemy Plugin do Polars
        df_final.write_database(table_name=table_name, connection=db_url, if_exists='replace')
        
        # Criação de índices com SQLAlchemy
        engine = create_engine(db_url)
        create_index_pk(engine, table_name, pk_col_db)
        
        return True

    except Exception as e:
        logger.error(f"❌ Erro Polars em {csv_filename}: {e}")
        return False

def refresh_sql_model():
    """Recria as Dimensões com os nomes exatos solicitados."""
    logger.info("🏗️ Atualizando Modelo de Dados (Full Polars)...")
    
    # Criação dos índices na tabela de Fato
    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_data_ref ON "{TABLE_NAME}" (data_ref);'))
        conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_municipio ON "{TABLE_NAME}" (municipio_codigo);'))
        conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_subclasse ON "{TABLE_NAME}" (subclasse_codigo);'))

    # Criação das Dimensões Estáticas e CSVs (Passando DB_URL)
    create_static_dimension(DB_URL, "dSexo", MAP_SEXO, "sexo_codigo", "sexo_descricao")
    create_static_dimension(DB_URL, "dRaca", MAP_RACA, "raca_cor_codigo", "raca_cor_desc")

    map_atividade = {"subclasse_codigo": "subclasse_codigo", "subclasse_descricao": "subclasse_descricao", "secao_codigo": "secao_codigo", "secao_descricao": "secao_nome"}
    create_csv_dimension_polars(DB_URL, "dAtividade", "cnae.csv", map_atividade, "subclasse_codigo")

    map_localidade = {"municipio_codigo_6": "municipio_codigo", "municipio_nome": "municipio_nome", "uf_sigla": "uf_sigla"}
    create_csv_dimension_polars(DB_URL, "dLocalidade", "municipios.csv", map_localidade, "municipio_codigo")

    map_escolaridade = {"codigo": "grau_instrucao_codigo", "descricao": "grau_instrucao_desc"}
    create_csv_dimension_polars(DB_URL, "dEscolaridade", "grau_instrucao.csv", map_escolaridade, "grau_instrucao_codigo")

    map_tipo_mov = {"codigo": "tipo_movimentacao_codigo", "descricao": "tipo_movimentacao_desc"}
    create_csv_dimension_polars(DB_URL, "dTipoMovimentacao", "tipo_movimentacao.csv", map_tipo_mov, "tipo_movimentacao_codigo")
    
    map_categoria = {"codigo": "categoria_codigo", "descricao": "categoria_desc"}
    create_csv_dimension_polars(DB_URL, "dCategoria", "categoria.csv", map_categoria, "categoria_codigo")
    
    map_secao = {"secao_codigo": "secao_codigo", "atividade_economica": "secao_nome"}
    create_csv_dimension_polars(DB_URL, "dSecao", "secoes.csv", map_secao, "secao_codigo")

    logger.info("✅ Modelo de dados 100% atualizado, indexado e otimizado com Polars!")

def cleanup_stale_files(keep_last=2):
    """Remove arquivos antigos processados para não lotar o disco."""
    types = ["CAGEDMOV", "CAGEDFOR", "CAGEDEXC"]
    for ftype in types:
        pattern = os.path.join(PROCESSED_DIR, f"{ftype}_*.csv")
        files = glob.glob(pattern)
        files.sort(reverse=True)
        if len(files) > keep_last:
            for f in files[keep_last:]:
                try: 
                    os.remove(f)
                    logger.info(f"🧹 Removido {os.path.basename(f)}")
                except: 
                    pass

def run_pipeline(year, month, force_download=False):
    """Orquestra o download, processamento e injeção do CAGED."""
    processor = CagedProcessor()
    files_to_process = ["CAGEDMOV", "CAGEDFOR", "CAGEDEXC"]
    
    for ftype in files_to_process:
        try:
            ftp = FTPClient()
            
            # 1. Download
            local_path = ftp.download_file(year, month, ftype, RAW_ZIP_DIR, force=force_download)
            if not local_path: continue

            # 2. Extração
            txt_path = processor.extract_file(local_path)
            if not txt_path: continue

            # 3. Tratamento (Polars)
            csv_filename = f"{ftype}_{year}{month}_sp.csv"
            processed_path = processor.process_data(txt_path, csv_filename, year, month, ftype)

            # 4. Injeção (Camada Lakehouse)
            data_ref_iso = f"{year}-{month}-01"
            df_processed = pl.read_csv(processed_path, separator=';')
            
            condicoes_limpeza = {
                "data_arquivo": data_ref_iso,
                "tipo_arquivo": ftype
            }

            # Envia para o motor de alta performance
            load_to_database(
                df=df_processed, 
                table_name=TABLE_NAME, 
                db_url=DB_URL, 
                delete_conditions=condicoes_limpeza
            )
            
            # Limpeza do txt cru extraído
            if os.path.exists(txt_path): 
                os.remove(txt_path)
                
        except Exception as e:
            logger.error(f"❌ Falha em {ftype}: {e}"); raise e

    cleanup_stale_files()