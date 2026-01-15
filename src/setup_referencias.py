import os
import requests
import pandas as pd
import unicodedata

# Configuração de Pastas
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.basename(BASE_DIR) == 'src':
    BASE_DIR = os.path.dirname(BASE_DIR)

REFS_DIR = os.path.join(BASE_DIR, 'data', 'refs')
if not os.path.exists(REFS_DIR): os.makedirs(REFS_DIR)

print("🚀 Criando tabelas de referência (Versão 3.0 - Join por Código)...")

# ==============================================================================
# 1. TABELA MÃE DE SETORES (Agora mapeada por CÓDIGO - Letra)
# ==============================================================================
print("📋 Gerando Tabela Mãe de Setores (secoes.csv)...")

# Mapeamento Oficial CNAE 2.0 (Códigos A-U)
# Isso é o coração da correção: Usamos a LETRA como chave.
dados_secoes = [
    # A: Agricultura
    {"secao_codigo": "A", "atividade_economica": "Agropecuária"},
    
    # B, C, D, E: Indústria
    {"secao_codigo": "B", "atividade_economica": "Indústria"},
    {"secao_codigo": "C", "atividade_economica": "Indústria"},
    {"secao_codigo": "D", "atividade_economica": "Indústria"},
    {"secao_codigo": "E", "atividade_economica": "Indústria"},
    
    # F: Construção
    {"secao_codigo": "F", "atividade_economica": "Construção"},
    
    # G: Comércio
    {"secao_codigo": "G", "atividade_economica": "Comércio"},
    
    # H até U: Serviços
    {"secao_codigo": "H", "atividade_economica": "Serviços"},
    {"secao_codigo": "I", "atividade_economica": "Serviços"},
    {"secao_codigo": "J", "atividade_economica": "Serviços"},
    {"secao_codigo": "K", "atividade_economica": "Serviços"},
    {"secao_codigo": "L", "atividade_economica": "Serviços"},
    {"secao_codigo": "M", "atividade_economica": "Serviços"},
    {"secao_codigo": "N", "atividade_economica": "Serviços"},
    {"secao_codigo": "O", "atividade_economica": "Serviços"},
    {"secao_codigo": "P", "atividade_economica": "Serviços"},
    {"secao_codigo": "Q", "atividade_economica": "Serviços"},
    {"secao_codigo": "R", "atividade_economica": "Serviços"},
    {"secao_codigo": "S", "atividade_economica": "Serviços"},
    {"secao_codigo": "T", "atividade_economica": "Serviços"},
    {"secao_codigo": "U", "atividade_economica": "Serviços"},
    
    # Z: Tratamento de erros
    {"secao_codigo": "Z", "atividade_economica": "Não Identificado"} 
]

df_secoes = pd.DataFrame(dados_secoes)
# Salva com separador ; para consistência
df_secoes.to_csv(os.path.join(REFS_DIR, 'secoes.csv'), sep=';', index=False)
print("✅ Tabela 'secoes.csv' recriada usando CÓDIGOS (Letras)!")

# ==============================================================================
# 2. DOWNLOAD CNAE (API IBGE V2) - Garantir que temos o Código da Seção
# ==============================================================================
print("⬇️ Baixando CNAE Subclasses (Isso pode demorar uns 30 segundos)...")
url_cnae = "https://servicodados.ibge.gov.br/api/v2/cnae/subclasses"

try:
    resp = requests.get(url_cnae)
    data = resp.json()
    lista_cnae = []
    
    for item in data:
        try:
            # Navega na hierarquia do IBGE para pegar a Seção (Letra)
            secao = item['classe']['grupo']['divisao']['secao']
            
            cnae_limpo = item['id'].replace("-", "").replace("/", "").replace(".", "")
            
            lista_cnae.append({
                'subclasse_codigo': int(cnae_limpo),
                'subclasse_descricao': item['descricao'],
                'secao_codigo': secao['id'],          # <--- O CAMPO VITAL (A, B, C...)
                'secao_descricao': secao['descricao'] 
            })
        except: continue
        
    df_cnae = pd.DataFrame(lista_cnae)
    path_cnae = os.path.join(REFS_DIR, 'cnae.csv')
    df_cnae.to_csv(path_cnae, sep=';', index=False)
    print(f"✅ Tabela CNAE criada com {len(df_cnae)} atividades e seus códigos de seção.")

except Exception as e:
    print(f"❌ Erro crítico ao baixar CNAE: {e}")

# ==============================================================================
# 3. DOWNLOAD MUNICÍPIOS (IBGE)
# ==============================================================================
print("⬇️ Baixando Municípios...")
try:
    resp = requests.get("https://servicodados.ibge.gov.br/api/v1/localidades/municipios")
    data = resp.json()
    lista_mun = []
    for item in data:
        try:
            uf = item['microrregiao']['mesorregiao']['UF']
            lista_mun.append({
                'municipio_codigo': int(item['id']),
                'municipio_nome': item['nome'],
                'uf_codigo': int(uf['id']),
                'uf_sigla': uf['sigla']
            })
        except: continue
    
    df_mun = pd.DataFrame(lista_mun)
    # Coluna auxiliar de 6 digitos
    df_mun['municipio_codigo_6'] = df_mun['municipio_codigo'].astype(str).str.slice(0, 6).astype(int)
    
    df_mun.to_csv(os.path.join(REFS_DIR, 'municipios.csv'), sep=';', index=False)
    print(f"✅ Municípios baixados.")
except Exception as e: print(f"❌ Erro Municípios: {e}")

# ==============================================================================
# 4. TABELAS FIXAS (Manuais)
# ==============================================================================
print("⚙️ Gerando tabelas manuais (Instrução, Categoria, Sexo, Raça)...")

# Grau de Instrução
instrucao_data = [
    { 'codigo': 1, 'descricao': 'Analfabeto' },
    { 'codigo': 2, 'descricao': 'Até 5ª Incompleto' },
    { 'codigo': 3, 'descricao': '5ª Completo Fundamental' },
    { 'codigo': 4, 'descricao': '6ª a 9ª Fundamental' },
    { 'codigo': 5, 'descricao': 'Fundamental Completo' },
    { 'codigo': 6, 'descricao': 'Médio Incompleto' },
    { 'codigo': 7, 'descricao': 'Médio Completo' },
    { 'codigo': 8, 'descricao': 'Superior Incompleto' },
    { 'codigo': 9, 'descricao': 'Superior Completo' },
    { 'codigo': 10, 'descricao': 'Mestrado' },
    { 'codigo': 11, 'descricao': 'Doutorado' },
    { 'codigo': 99, 'descricao': 'Não Identificado' }
]
pd.DataFrame(instrucao_data).to_csv(os.path.join(REFS_DIR, 'grau_instrucao.csv'), sep=';', index=False)

# Categoria
categoria_data = [
    { 'codigo': 101, 'descricao': 'Empregado - Geral' },
    { 'codigo': 102, 'descricao': 'Empregado - Rural' },
    { 'codigo': 103, 'descricao': 'Aprendiz' },
    { 'codigo': 104, 'descricao': 'Doméstico' },
    { 'codigo': 105, 'descricao': 'Contrato a Termo' },
    { 'codigo': 106, 'descricao': 'Trabalhador Temporário' },
    { 'codigo': 107, 'descricao': 'Contrato Verde e Amarelo' },
    { 'codigo': 111, 'descricao': 'Intermitente' },
    { 'codigo': 999, 'descricao': 'Não Identificado' }
]
pd.DataFrame(categoria_data).to_csv(os.path.join(REFS_DIR, 'categoria.csv'), sep=';', index=False)

# Tipo Movimentação
movimentacao_data = [
    { 'codigo': 10, 'descricao': 'Admissão por Primeiro Emprego' },
    { 'codigo': 20, 'descricao': 'Admissão por Reemprego' },
    { 'codigo': 25, 'descricao': 'Admissão por Contrato Determinado' },
    { 'codigo': 35, 'descricao': 'Admissão por Reintegração' },
    { 'codigo': 97, 'descricao': 'Admissão por Transferência' },
    { 'codigo': 31, 'descricao': 'Desligamento sem Justa Causa' },
    { 'codigo': 32, 'descricao': 'Desligamento com Justa Causa' },
    { 'codigo': 40, 'descricao': 'Pedido de Demissão' },
    { 'codigo': 43, 'descricao': 'Término Contrato Determinado' },
    { 'codigo': 45, 'descricao': 'Término de Contrato' },
    { 'codigo': 50, 'descricao': 'Desligamento por Aposentadoria' },
    { 'codigo': 60, 'descricao': 'Desligamento por Morte' },
    { 'codigo': 90, 'descricao': 'Desligamento por Acordo' },
    { 'codigo': 98, 'descricao': 'Desligamento por Transferência' }
]
pd.DataFrame(movimentacao_data).to_csv(os.path.join(REFS_DIR, 'tipo_movimentacao.csv'), sep=';', index=False)

print("✅ Setup finalizado com sucesso!")