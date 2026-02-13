import polars as pl
import logging
from sqlalchemy import create_engine, text, inspect

try:
    from .config import DB_URL, TABLE_NAME
except ImportError:
    from src.config import DB_URL, TABLE_NAME

def load_to_database(csv_source, data_ref_iso, file_type):
    """
    Carrega o CSV para o Banco.
    Apaga dados anteriores usando 'data_arquivo' para garantir que não haja duplicidade.
    """
    engine = create_engine(DB_URL)
    
    # Lê com Polars
    df = pl.read_csv(csv_source, separator=';', infer_schema_length=10000)
    pandas_df = df.to_pandas()

    with engine.begin() as conn:
        inspector = inspect(engine)
        
        # 1. Evolução de Esquema (Cria colunas novas)
        if inspector.has_table(TABLE_NAME):
            columns_in_db = [c['name'] for c in inspector.get_columns(TABLE_NAME)]
            
            for col in pandas_df.columns:
                if col not in columns_in_db:
                    print(f"✨ Adicionando nova coluna ao banco: {col}")
                    
                    if col in ['salario', 'valor_salario_fixo']: col_type = "FLOAT"
                    elif col in ['saldo_movimentacao']: col_type = "BIGINT"
                    elif "data" in col: col_type = "TIMESTAMP" # Garante que data_ref seja Data
                    else: col_type = "TEXT"
                    
                    conn.execute(text(f'ALTER TABLE {TABLE_NAME} ADD COLUMN "{col}" {col_type};'))
            
            # 2. Limpeza Idempotente (Usa a data do ARQUIVO para limpar)
            conn.execute(
                text(f"DELETE FROM {TABLE_NAME} WHERE data_arquivo = :dt AND tipo_arquivo = :tp"),
                {"dt": data_ref_iso, "tp": file_type}
            )
        
        print(f"🚀 Inserindo {len(df)} registros na principal...")
        pandas_df.to_sql(TABLE_NAME, conn, if_exists='append', index=False, chunksize=10000)
        
    return True