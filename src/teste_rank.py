import pandas as pd
import os

# --- CONFIGURAÇÕES ---
DIRETORIO_DADOS = r"D:\Projetos\extracao_caged\data\processed"
ARQUIVO_REFS = r"D:\Projetos\extracao_caged\data\refs\municipios.csv"
COMPETENCIA_ALVO = 202512

def carregar_csv(caminho):
    if os.path.exists(caminho):
        # Tenta ler com ; (padrão CAGED novo), se falhar tenta ,
        try:
            return pd.read_csv(caminho, sep=';', encoding='utf-8', dtype=str)
        except:
            return pd.read_csv(caminho, sep=',', encoding='utf-8', dtype=str)
    return pd.DataFrame()

print(f"--- GERANDO TOP 50 MUNICÍPIOS (ADMISSÕES > SALDO) ---")

# 1. CARGA E UNIFICAÇÃO
df_mov = carregar_csv(os.path.join(DIRETORIO_DADOS, f"CAGEDMOV_{COMPETENCIA_ALVO}_sp.csv"))
df_for = carregar_csv(os.path.join(DIRETORIO_DADOS, f"CAGEDFOR_{COMPETENCIA_ALVO}_sp.csv"))
df_exc = carregar_csv(os.path.join(DIRETORIO_DADOS, f"CAGEDEXC_{COMPETENCIA_ALVO}_sp.csv"))

# Padronização de Data
for df in [df_mov, df_for, df_exc]:
    if not df.empty and 'competencia_mov' in df.columns:
        df['competencia_mov'] = df['competencia_mov'].astype(str).str.replace('-', '').str.slice(0, 6)

# Unificação
df_bruto = pd.concat([df_mov, df_for], ignore_index=True)

# Remoção de Exclusões
if not df_exc.empty:
    colunas_chave = [c for c in df_bruto.columns if c in df_exc.columns]
    df_caged = df_bruto.merge(df_exc[colunas_chave], on=colunas_chave, how='outer', indicator=True)
    df_caged = df_caged[df_caged['_merge'] == 'left_only'].drop(columns=['_merge'])
else:
    df_caged = df_bruto

# 2. PREPARAÇÃO
df_caged['saldo_movimentacao'] = pd.to_numeric(df_caged['saldo_movimentacao'], errors='coerce').fillna(0).astype(int)

# Filtra Mês Alvo
df_mes = df_caged[df_caged['competencia_mov'] == str(COMPETENCIA_ALVO)].copy()

# Cria contador de admissões (Admissão = 1, Desligamento = 0 para contagem)
df_mes['contagem_admissao'] = df_mes['saldo_movimentacao'].apply(lambda x: 1 if x == 1 else 0)

# 3. CRUZAMENTO COM NOMES
df_municipios = carregar_csv(ARQUIVO_REFS)

# Prepara chaves 6 dígitos
df_mes['chave_join'] = df_mes['municipio_codigo'].astype(str).str.slice(0, 6)
col_cod_ref = df_municipios.columns[0]
col_nome_ref = df_municipios.columns[1]
df_municipios['chave_join'] = df_municipios[col_cod_ref].astype(str).str.slice(0, 6)

df_final = df_mes.merge(df_municipios, on='chave_join', how='left')
df_final[col_nome_ref] = df_final[col_nome_ref].fillna('DESCONHECIDO')

# 4. AGRUPAMENTO E RANKING
ranking = df_final.groupby(col_nome_ref).agg({
    'contagem_admissao': 'sum',      # Total Admissões
    'saldo_movimentacao': 'sum'      # Saldo (Critério Desempate)
}).reset_index()

ranking.columns = ['Municipio', 'Total_Admissoes', 'Saldo_Liquido']

# Ordena: 1º Admissões (Maior), 2º Saldo (Maior)
ranking = ranking.sort_values(by=['Total_Admissoes', 'Saldo_Liquido'], ascending=[False, False])

# Numera as posições
ranking['Posicao'] = range(1, len(ranking) + 1)

# 5. EXIBIÇÃO TOP 50
pd.set_option('display.max_rows', 60) # Garante que o Python mostre tudo sem cortar
pd.set_option('display.width', 1000)

print("\n" + "="*70)
print(f"TOP 50 CIDADES - SP ({COMPETENCIA_ALVO})")
print("="*70)
print(ranking.head(50).to_string(index=False))

print("\n" + "="*70)
# Mostra Botucatu em destaque novamente caso tenha ficado fora do Top 50 (ex: 51º)
botucatu = ranking[ranking['Municipio'].astype(str).str.contains("Botucatu", case=False, na=False)]
if not botucatu.empty:
    print("DESTACANDO BOTUCATU:")
    print(botucatu.to_string(index=False))