import polars as pl
import logging
import io
from sqlalchemy import create_engine, text, inspect
from typing import Dict, Any

# Mapeamento de Tipos Polars -> PostgreSQL para evolução de esquema
POLARS_TO_PG_TYPE = {
    pl.Int8: "SMALLINT", pl.Int16: "SMALLINT", pl.Int32: "INTEGER", pl.Int64: "BIGINT",
    pl.Float32: "REAL", pl.Float64: "DOUBLE PRECISION",
    pl.Boolean: "BOOLEAN", pl.Date: "DATE", pl.Datetime: "TIMESTAMP",
    pl.Utf8: "TEXT" 
}

def load_to_database(df: pl.DataFrame, table_name: str, db_url: str, delete_conditions: Dict[str, Any] = None):
    """
    Motor de Carga Universal (Camada Gold).
    Injeta DataFrames Polars no Postgres via Bulk Insert (COPY).
    """
    if df.is_empty():
        logging.warning(f"⚠️ DataFrame vazio para a tabela '{table_name}'. Nenhuma carga realizada.")
        return True

    engine = create_engine(db_url)

    # 1. GARANTIA DE TABELA BASE (Transação Isolada)
    # Garante que a tabela tem uma Primary Key (id) logo no nascimento
    with engine.begin() as conn:
        conn.execute(text(f'CREATE TABLE IF NOT EXISTS "{table_name}" (id SERIAL PRIMARY KEY);'))

    # 2. PREPARAÇÃO DO BANCO (Evolução de Esquema e Limpeza)
    with engine.begin() as conn:
        inspector = inspect(engine)
        
        columns_in_db = [c['name'] for c in inspector.get_columns(table_name)]
        for col_name, polars_dtype in zip(df.columns, df.dtypes):
            if col_name not in columns_in_db:
                pg_type = POLARS_TO_PG_TYPE.get(type(polars_dtype), "TEXT")
                logging.info(f"✨ Nova coluna detetada: '{col_name}' ({pg_type}) em '{table_name}'. Criando...")
                conn.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {pg_type};'))
        
        if delete_conditions:
            where_clauses = " AND ".join([f"{k} = :{k}" for k in delete_conditions.keys()])
            logging.info(f"🧹 Limpando dados históricos do período...")
            conn.execute(text(f'DELETE FROM "{table_name}" WHERE {where_clauses}'), delete_conditions)

    # 3. CARGA MASSIVA (Bulk Insert via COPY)
    logging.info(f"🚀 Preparando inserção de {df.height} registos...")
    
    # Usando BytesIO porque o Polars escreve em bytes nativos para maior velocidade
    csv_buffer = io.BytesIO()
    df.write_csv(csv_buffer)
    csv_buffer.seek(0)

    # O Pulo do Gato: Captura as colunas exatas do DataFrame geradas pelo Polars
    # Isto ensina o Postgres a injetar os dados apenas nestas colunas, ignorando o "id"
    columns_list = '", "'.join(df.columns)

    # Acedendo ao motor cru do Psycopg2
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cursor:
            # COPY inteligente: especificamos as colunas dinamicamente
            copy_sql = f'COPY "{table_name}" ("{columns_list}") FROM stdin WITH CSV HEADER DELIMITER as \',\''
            cursor.copy_expert(sql=copy_sql, file=csv_buffer)
        raw_conn.commit()
        logging.info(f"✅ Carga massiva finalizada com sucesso em '{table_name}'.")
    except Exception as e:
        raw_conn.rollback()
        logging.error(f"❌ Erro crítico no Bulk Insert: {e}")
        raise e
    finally:
        raw_conn.close()
        
    return True