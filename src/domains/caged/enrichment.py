import polars as pl
import os
from .config import REFS_DIR

def load_ref_table(filename: str):
    """Carrega tabela auxiliar da pasta refs de forma preguiçosa (Lazy)."""
    path = os.path.join(REFS_DIR, filename)
    if not os.path.exists(path):
        print(f"⚠️ Arquivo de referência não encontrado: {filename}. Certifique-se de que correu o setup_referencias.py")
        return None
    # scan_csv para performance
    return pl.scan_csv(path, separator=';', infer_schema_length=10000)

def apply_enrichment(df_lazy: pl.LazyFrame) -> pl.DataFrame:
    """
    Realiza os Joins com as tabelas de referência e limpa colunas técnicas.
    """
    print("🔄 Cruzando dados com tabelas de referência (IBGE/Caged)...")

    # 1. Carregar Referências
    ref_municipios = load_ref_table('municipios.csv')
    ref_cnae = load_ref_table('cnae.csv')
    ref_instrucao = load_ref_table('grau_instrucao.csv')
    ref_categoria = load_ref_table('categoria.csv')
    ref_tipo_mov = load_ref_table('tipo_movimentacao.csv')

    # ==========================================================================
    # 1. MUNICÍPIOS (Padronização para 6 dígitos)
    # ==========================================================================
    # Cria uma chave temporária de 6 dígitos no CAGED para garantir o match com o IBGE
    df_lazy = df_lazy.with_columns(
        pl.col('municipio_codigo').cast(pl.Utf8).str.slice(0, 6).cast(pl.Int64).alias('municipio_join_key')
    )

    if ref_municipios is not None:
        # Prepara a REFERÊNCIA: Usa a coluna de 6 dígitos criada no setup
        ref_mun_clean = ref_municipios.select([
            pl.col('municipio_codigo_6').cast(pl.Int64).alias('municipio_join_key_ref'), 
            pl.col('municipio_nome'), 
            pl.col('uf_sigla')
        ])

        df_lazy = df_lazy.join(
            ref_mun_clean,
            left_on='municipio_join_key',
            right_on='municipio_join_key_ref',
            how='left'
        )

    # ==========================================================================
    # 2. CNAE & SEÇÃO
    # ==========================================================================
    if ref_cnae is not None:
        # Garante int64 para o join
        ref_cnae = ref_cnae.with_columns(pl.col('subclasse_codigo').cast(pl.Int64))

        df_lazy = df_lazy.join(
            ref_cnae,
            left_on='subclasse_codigo',
            right_on='subclasse_codigo',
            how='left',
            suffix='_ref' # Se houver conflito, adiciona este sufixo
        )
        
        # Garante que temos a descrição da secção numa coluna limpa
        # Se 'secao_descricao' veio do join, usamos ela.
        df_lazy = df_lazy.with_columns(
            pl.col('secao_descricao').alias('secao_nome')
        )

    # ==========================================================================
    # 3. DEMAIS TABELAS (Joins Simples)
    # ==========================================================================
    
    # Instrução
    if ref_instrucao is not None:
        df_lazy = df_lazy.join(
            ref_instrucao.select([pl.col('codigo').cast(pl.Int64), pl.col('descricao').alias('grau_instrucao_desc')]),
            left_on='grau_instrucao_codigo', right_on='codigo', how='left'
        )

    # Categoria
    if ref_categoria is not None:
        df_lazy = df_lazy.join(
            ref_categoria.select([pl.col('codigo').cast(pl.Int64), pl.col('descricao').alias('categoria_desc')]),
            left_on='categoria_codigo', right_on='codigo', how='left'
        )

    # Movimentação
    if ref_tipo_mov is not None:
        df_lazy = df_lazy.join(
            ref_tipo_mov.select([pl.col('codigo').cast(pl.Int64), pl.col('descricao').alias('tipo_movimentacao_desc')]),
            left_on='tipo_movimentacao_codigo', right_on='codigo', how='left'
        )

    # ==========================================================================
    # 4. MAPEAMENTOS MANUAIS (Sexo e Raça)
    # ==========================================================================
    df_lazy = df_lazy.with_columns([
        pl.when(pl.col("sexo_codigo") == 1).then(pl.lit("Masculino"))
          .when(pl.col("sexo_codigo") == 3).then(pl.lit("Feminino"))
          .otherwise(pl.lit("Não Informado")).alias("sexo_descricao"),
          
        pl.when(pl.col("raca_cor_codigo") == 1).then(pl.lit("Branca"))
          .when(pl.col("raca_cor_codigo") == 2).then(pl.lit("Preta"))
          .when(pl.col("raca_cor_codigo") == 3).then(pl.lit("Parda"))
          .when(pl.col("raca_cor_codigo") == 4).then(pl.lit("Amarela"))
          .when(pl.col("raca_cor_codigo") == 5).then(pl.lit("Indígena"))
          .otherwise(pl.lit("Não Informado")).alias("raca_cor_desc")
    ])

    # ==========================================================================
    # 5. LIMPEZA FINAL (DROP)
    # ==========================================================================
    # Remove colunas técnicas usadas apenas para o join ou duplicadas
    cols_to_drop = [
        'municipio_join_key', 
        'municipio_join_key_ref', 
        'secao_codigo_ref',
        'secao_descricao' # Removemos esta pois criámos a 'secao_nome' (ou vice-versa, para não ter duas)
    ]
    
    # Executa e obtém o Schema para evitar erro ao tentar apagar coluna que não existe
    df_final = df_lazy.collect()
    existing_cols = df_final.columns
    
    # Filtra apenas as colunas que realmente existem para apagar
    cols_existing_to_drop = [c for c in cols_to_drop if c in existing_cols]
    
    if cols_existing_to_drop:
        df_final = df_final.drop(cols_existing_to_drop)

    return df_final