import os
import logging
import psycopg2
from sqlalchemy import create_engine

try:
    from .config import DB_URL, TABLE_NAME
except ImportError:
    from src.config import DB_URL, TABLE_NAME

logger = logging.getLogger(__name__)

def create_table_if_not_exists(conn):
    # Nova estrutura baseada na sua solicitação
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        uf_codigo INTEGER,
        municipio_codigo INTEGER,
        secao_codigo VARCHAR(5),
        subclasse_codigo BIGINT,
        saldo_movimentacao INTEGER,
        categoria_codigo INTEGER,
        grau_instrucao_codigo INTEGER,
        idade INTEGER,
        raca_cor_codigo INTEGER,
        sexo_codigo INTEGER,
        tipo_movimentacao_codigo INTEGER,
        salario DECIMAL(12,2),
        data_ref DATE,
        data_proc DATE,
        municipio_nome VARCHAR(255),
        uf_sigla CHAR(2),
        subclasse_descricao TEXT,
        secao_nome VARCHAR(255),
        grau_instrucao_desc VARCHAR(255),
        categoria_desc VARCHAR(255),
        tipo_movimentacao_desc VARCHAR(255),
        sexo_descricao VARCHAR(50),
        raca_cor_desc VARCHAR(50)
    );
    """
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
            conn.commit()
        return True
    except Exception as e:
        logger.error(f"❌ Erro criando tabela: {e}")
        return False

def load_to_database(csv_full_path):
    if not os.path.exists(csv_full_path): return False

    try:
        engine = create_engine(DB_URL)
        conn = engine.raw_connection()
        
        if not create_table_if_not_exists(conn):
            conn.close()
            return False

        cur = conn.cursor()
        logger.info(f"⏳ Carga COPY para {TABLE_NAME}...")

        with open(csv_full_path, 'r', encoding='utf-8') as f:
            next(f) # Pula Header
            # Atenção: DELIMITER ';' pois o Processor salvou assim
            sql = f"COPY {TABLE_NAME} FROM STDIN WITH CSV DELIMITER ';' QUOTE '\"' NULL ''"
            cur.copy_expert(sql, f)

        conn.commit()
        logger.info(f"✅ Sucesso! {cur.rowcount} linhas.")
        cur.close()
        conn.close()
        return True

    except Exception as e:
        logger.error(f"❌ Erro Loader: {e}")
        return False