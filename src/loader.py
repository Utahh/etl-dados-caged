import polars as pl
import pandas as pd
from sqlalchemy import create_engine, text
import os
from .config import DB_URL, TABLE_NAME

def load_to_database(csv_source, data_ref_iso):
    """
    Carrega os dados no PostgreSQL.
    Usa Polars para leitura (rápido) e Pandas para escrita (compatível com Transação SQLAlchemy).
    """
    engine = create_engine(DB_URL)
    
    # 1. Leitura Rápida com Polars
    if isinstance(csv_source, str):
        print(f"📂 Lendo CSV com Polars: {csv_source}")
        try:
            # Tenta ler com ;
            df_polars = pl.read_csv(csv_source, separator=';', infer_schema_length=10000, ignore_errors=True)
        except Exception as e:
            print(f"⚠️ Aviso: Tentando ler com vírgula. Erro anterior: {e}")
            df_polars = pl.read_csv(csv_source, separator=',', infer_schema_length=10000)
    else:
        df_polars = csv_source

    try:
        # Iniciamos a Transação (Tudo ou Nada)
        with engine.begin() as conn:
            # 2. IDEMPOTÊNCIA (Limpeza)
            print(f"🧹 [Idempotência] Apagando dados antigos de {data_ref_iso}...")
            conn.execute(
                text(f"DELETE FROM {TABLE_NAME} WHERE data_ref = :dt"),
                {"dt": data_ref_iso}
            )

            # 3. CARGA VIA PANDAS (Fallback Seguro)
            # Convertemos para Pandas aqui porque o to_sql do Pandas aceita a conexão 'conn'
            # sem tentar recriar a engine, evitando o erro '_instantiate_plugins'.
            print(f"🔄 Convertendo para Pandas para inserção segura...")
            df_pandas = df_polars.to_pandas()
            
            print(f"🚀 Inserindo {len(df_pandas)} linhas no banco...")
            
            df_pandas.to_sql(
                name=TABLE_NAME,
                con=conn,
                if_exists='append',
                index=False,
                chunksize=10000 # Insere em lotes para não estourar memória do banco
            )
            
            print(f"✅ Sucesso: {data_ref_iso} carregado e commitado!")
            return True

    except Exception as e:
        print(f"❌ Erro crítico no loader: {e}")
        raise e