import os
import polars as pl
import logging
from sqlalchemy import create_engine, text, inspect
import pandas as pd

logger = logging.getLogger(__name__)

try:
    from .config import PROCESSED_DIR, DB_URL, TABLE_NAME
except ImportError:
    from src.config import PROCESSED_DIR, DB_URL, TABLE_NAME

def get_db_engine():
    return create_engine(os.getenv('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN', DB_URL), future=True)

def load_to_database(csv_filename: str):
    csv_path = os.path.join(PROCESSED_DIR, csv_filename)
    if not os.path.exists(csv_path):
        logger.error(f"❌ Arquivo não encontrado: {csv_path}")
        return False

    logger.info(f"📥 Iniciando Loader Inteligente para {csv_filename}...")
    
    try:
        engine = get_db_engine()
        
        # 1. Analisa o CSV (cabeçalho)
        df_head = pl.read_csv(csv_path, separator=';', n_rows=1, ignore_errors=True)
        csv_columns = sorted(df_head.columns)
        
        if 'competencia_declarada' not in df_head.columns:
             raise Exception("CSV inválido: Faltando coluna competencia_declarada")
        
        data_ref = df_head['competencia_declarada'][0]

        with engine.begin() as conn:
            inspector = inspect(engine)
            
            # 2. Verifica se a tabela existe e se a ESTRUTURA mudou
            if inspector.has_table(TABLE_NAME):
                db_columns = sorted([c['name'] for c in inspector.get_columns(TABLE_NAME)])
                
                # COMPARAÇÃO INTELIGENTE DE COLUNAS
                if csv_columns != db_columns:
                    logger.warning("⚠️ MUDANÇA DE ESTRUTURA DETECTADA!")
                    logger.warning(f"CSV: {csv_columns}")
                    logger.warning(f"DB:  {db_columns}")
                    logger.warning("🧨 Executando DROP TABLE automático para recriar estrutura correta...")
                    conn.execute(text(f"DROP TABLE {TABLE_NAME}"))
                    table_exists = False
                else:
                    logger.info("✅ Estrutura da tabela está correta.")
                    logger.info(f"🔄 Limpando dados do mês {data_ref} para evitar duplicidade...")
                    conn.execute(text(f"DELETE FROM {TABLE_NAME} WHERE competencia_declarada = :d"), {"d": data_ref})
                    table_exists = True
            else:
                table_exists = False
            
            # 3. Se tabela não existe (ou foi dropada), cria a casca
            if not table_exists:
                logger.info(f"🆕 Criando tabela '{TABLE_NAME}' baseada no CSV...")
                # Lê amostra maior para o Pandas inferir tipos (int vs text)
                df_sample = pd.read_csv(csv_path, sep=';', nrows=1000)
                df_sample.head(0).to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
                logger.info("✅ Tabela criada com sucesso.")

        # 4. Carga TURBO (COPY)
        logger.info("🚀 Executando COPY stream (Alta Performance)...")
        raw_conn = engine.raw_connection()
        try:
            with raw_conn.cursor() as cursor:
                with open(csv_path, 'r', encoding='utf-8') as f:
                    cursor.copy_expert(
                        sql=f"COPY {TABLE_NAME} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ';', NULL '')",
                        file=f
                    )
            raw_conn.commit()
            logger.info(f"✅ Dados carregados com sucesso!")
        finally:
            raw_conn.close()

        # 5. Índices (Performance para o Dashboard)
        with engine.begin() as conn:
            logger.info("⚡ Otimizando Índices...")
            
            # Recarrega colunas para garantir que o índice seja criado no campo certo
            inspector = inspect(engine)
            existing_columns = [c['name'] for c in inspector.get_columns(TABLE_NAME)]
            
            cmds = [
                f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_data ON {TABLE_NAME} (competencia_declarada);",
                f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_muni ON {TABLE_NAME} (municipio_codigo);"
            ]
            
            # Índice Flexível (Cria no que estiver disponível)
            if 'secao_descricao' in existing_columns:
                cmds.append(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_setor ON {TABLE_NAME} (secao_descricao);")
            elif 'secao_nome' in existing_columns:
                cmds.append(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_setor_raw ON {TABLE_NAME} (secao_nome);")
            
            if 'atividade_economica' in existing_columns:
                cmds.append(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_atividade ON {TABLE_NAME} (atividade_economica);")

            for cmd in cmds:
                try: conn.execute(text(cmd))
                except: pass
        
        return True

    except Exception as e:
        logger.error(f"❌ Erro crítico no Loader: {e}")
        raise e