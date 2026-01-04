import os
import pandas as pd
from sqlalchemy import create_engine, text, inspect
import logging

logger = logging.getLogger(__name__)

try:
    from .config import PROCESSED_DIR, DB_URL, TABLE_NAME
except ImportError:
    from src.config import PROCESSED_DIR, DB_URL, TABLE_NAME

def get_db_engine():
    # Adicionamos future=True para garantir compatibilidade moderna, se necessário
    return create_engine(os.getenv('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN', DB_URL), future=True)

def create_performance_indexes(engine):
    """
    Cria índices no banco de dados para garantir que os dashboards
    de Botucatu e região carreguem em milissegundos.
    """
    logger.info("⚡ Otimizando banco de dados (Criando Índices)...")
    
    indexes = [
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_municipio ON {TABLE_NAME} (municipio_codigo);",
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_data ON {TABLE_NAME} (competencia_declarada);",
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_mun_data ON {TABLE_NAME} (municipio_codigo, competencia_declarada);",
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_setor ON {TABLE_NAME} (subclasse_codigo);",
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_saldo ON {TABLE_NAME} (saldo_movimentacao);"
    ]

    try:
        # CORREÇÃO AQUI: Usamos engine.begin() para transação automática
        with engine.begin() as conn:
            for sql in indexes:
                conn.execute(text(sql))
            # O commit ocorre automaticamente ao sair do bloco 'with'
        logger.info("⚡ Índices criados/verificados com sucesso!")
    except Exception as e:
        logger.warning(f"⚠️ Aviso: Não foi possível criar índices (pode ser permissão ou já existem): {e}")

def load_to_database(csv_filename: str):
    csv_path = os.path.join(PROCESSED_DIR, csv_filename)
    if not os.path.exists(csv_path):
        logger.error(f"❌ Arquivo não encontrado: {csv_path}")
        return False

    logger.info(f"📥 Iniciando carga de {csv_filename}...")
    try:
        engine = get_db_engine()
        df_header = pd.read_csv(csv_path, sep=';', nrows=1)
        
        if 'competencia_declarada' not in df_header.columns:
            raise Exception("CSV inválido: falta coluna 'competencia_declarada'.")
            
        data_ref = df_header['competencia_declarada'].iloc[0]
        
        # VERIFICAÇÃO DE TABELA
        inspector = inspect(engine)
        if inspector.has_table(TABLE_NAME):
            logger.info(f"🔄 Limpando dados antigos de {data_ref}...")
            # CORREÇÃO AQUI: Usamos engine.begin() e removemos o conn.commit() manual
            with engine.begin() as conn:
                conn.execute(text(f"DELETE FROM {TABLE_NAME} WHERE competencia_declarada = :d"), {"d": data_ref})
        else:
            logger.info(f"🆕 Tabela '{TABLE_NAME}' será criada agora.")

        chunk_size = 2000
        total_rows = 0
        for chunk in pd.read_csv(csv_path, sep=';', chunksize=chunk_size):
            chunk.columns = [c.lower() for c in chunk.columns]
            chunk.to_sql(name=TABLE_NAME, con=engine, if_exists='append', index=False, method='multi')
            total_rows += len(chunk)
            if total_rows % 20000 == 0: logger.info(f"   -> {total_rows} linhas...")

        logger.info(f"✅ Carga concluída! Total: {total_rows}")
        
        # --- ETAPA FINAL: OTIMIZAÇÃO ---
        create_performance_indexes(engine)
        # -------------------------------

        with open(csv_path.replace('.csv', '_REPORT.txt'), 'w') as f: f.write(f"OK: {total_rows}")
        return True

    except Exception as e:
        logger.error(f"❌ Erro crítico no Loader: {e}")
        raise e