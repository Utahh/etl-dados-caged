import os
import requests
import pandas as pd
import time
import unicodedata  # <--- Adicionado para normalização de texto

# Configuração de Pastas
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Garante que aponta para extracao_caged/data/refs, mesmo rodando de dentro de src
if os.path.basename(BASE_DIR) == 'src':
    BASE_DIR = os.path.dirname(BASE_DIR)

REFS_DIR = os.path.join(BASE_DIR, 'data', 'refs')

if not os.path.exists(REFS_DIR):
    os.makedirs(REFS_DIR)

print("🚀 Iniciando criação de tabelas de referência (Completo + Tabela Mãe)...")

def normalize(text):
    """Remove acentos e caracteres especiais para garantir o cruzamento (Join)"""
    if not isinstance(text, str): return str(text)
    return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8').lower().strip()

# ==============================================================================
# 1. TABELA MÃE DE SETORES (A Lógica Nova de Agrupamento)
# ==============================================================================
print("📋 Gerando Tabela Mãe de Setores Econômicos (Agrupamento)...")

# Mapeamento Oficial: Nome da Seção no Arquivo -> Atividade Econômica Resumida
dados_secoes = [
    # AGRICULTURA
    {"secao_nome": "AGRICULTURA, PECUÁRIA, PRODUÇÃO FLORESTAL, PESCA E AQÜICULTURA", "atividade_economica": "Agropecuária"},
    
    # INDÚSTRIA
    {"secao_nome": "INDÚSTRIAS EXTRATIVAS", "atividade_economica": "Indústria"},
    {"secao_nome": "INDÚSTRIAS DE TRANSFORMAÇÃO", "atividade_economica": "Indústria"},
    {"secao_nome": "ELETRICIDADE E GÁS", "atividade_economica": "Indústria"},
    {"secao_nome": "ÁGUA, ESGOTO, ATIVIDADES DE GESTÃO DE RESÍDUOS E DESCONTAMINAÇÃO", "atividade_economica": "Indústria"},
    
    # CONSTRUÇÃO
    {"secao_nome": "CONSTRUÇÃO", "atividade_economica": "Construção"},
    
    # COMÉRCIO
    {"secao_nome": "COMÉRCIO; REPARAÇÃO DE VEÍCULOS AUTOMOTORES E MOTOCICLETAS", "atividade_economica": "Comércio"},
    
    # SERVIÇOS (Todas as outras)
    {"secao_nome": "TRANSPORTE, ARMAZENAGEM E CORREIO", "atividade_economica": "Serviços"},
    {"secao_nome": "ALOJAMENTO E ALIMENTAÇÃO", "atividade_economica": "Serviços"},
    {"secao_nome": "INFORMAÇÃO E COMUNICAÇÃO", "atividade_economica": "Serviços"},
    {"secao_nome": "ATIVIDADES FINANCEIRAS, DE SEGUROS E SERVIÇOS RELACIONADOS", "atividade_economica": "Serviços"},
    {"secao_nome": "ATIVIDADES IMOBILIÁRIAS", "atividade_economica": "Serviços"},
    {"secao_nome": "ATIVIDADES PROFISSIONAIS, CIENTÍFICAS E TÉCNICAS", "atividade_economica": "Serviços"},
    {"secao_nome": "ATIVIDADES ADMINISTRATIVAS E SERVIÇOS COMPLEMENTARES", "atividade_economica": "Serviços"},
    {"secao_nome": "ADMINISTRAÇÃO PÚBLICA, DEFESA E SEGURIDADE SOCIAL", "atividade_economica": "Serviços"},
    {"secao_nome": "EDUCAÇÃO", "atividade_economica": "Serviços"},
    {"secao_nome": "SAÚDE HUMANA E SERVIÇOS SOCIAIS", "atividade_economica": "Serviços"},
    {"secao_nome": "ARTES, CULTURA, ESPORTE E RECREAÇÃO", "atividade_economica": "Serviços"},
    {"secao_nome": "OUTRAS ATIVIDADES DE SERVIÇOS", "atividade_economica": "Serviços"},
    {"secao_nome": "SERVIÇOS DOMÉSTICOS", "atividade_economica": "Serviços"},
    {"secao_nome": "ORGANISMOS INTERNACIONAIS E OUTRAS INSTITUIÇÕES EXTRATERRITORIAIS", "atividade_economica": "Serviços"},
    {"secao_nome": "NÃO IDENTIFICADO", "atividade_economica": "Não Identificado"}
]

df_secoes = pd.DataFrame(dados_secoes)
# Cria chave normalizada para o Join funcionar independente de maiúsculas/acentos
df_secoes['match_key'] = df_secoes['secao_nome'].apply(normalize)
df_secoes.to_csv(os.path.join(REFS_DIR, 'secoes.csv'), sep=';', index=False)
print("✅ Tabela 'secoes.csv' criada com sucesso.")


# ==============================================================================
# 2. TABELA DE MUNICÍPIOS E UF (Fonte: API IBGE V1)
# ==============================================================================
print("⬇️ Baixando Municípios do IBGE...")
url_mun = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"

try:
    response = requests.get(url_mun)
    data = response.json()
    lista_mun = []
    
    if isinstance(data, list):
        for item in data:
            try:
                micro = item.get('microrregiao', {})
                meso = micro.get('mesorregiao', {}) if micro else {}
                uf = meso.get('UF', {}) if meso else {}
                
                if not uf and 'microrregiao' in item and 'UF' in item['microrregiao']: 
                     uf = item['microrregiao']['UF']

                if uf:
                    lista_mun.append({
                        'municipio_codigo': int(item['id']),
                        'municipio_nome': item['nome'],
                        'uf_codigo': int(uf.get('id', 0)),
                        'uf_sigla': uf.get('sigla', '')
                    })
            except Exception as e_item:
                continue

        df_mun = pd.DataFrame(lista_mun)
        df_mun['municipio_codigo_6'] = df_mun['municipio_codigo'].astype(str).str.slice(0, 6).astype(int)
        df_mun = df_mun[df_mun['uf_codigo'] > 0]
        
        path_mun = os.path.join(REFS_DIR, 'municipios.csv')
        df_mun.to_csv(path_mun, sep=';', index=False)
        print(f"✅ Tabela 'municipios.csv' criada com {len(df_mun)} cidades.")
    else:
        print("❌ Erro: API do IBGE não retornou uma lista.")
except Exception as e:
    print(f"❌ Erro crítico ao baixar Municípios: {e}")


# ==============================================================================
# 3. TABELA CNAE (Subclasses e Seções) (Fonte: API IBGE V2)
# ==============================================================================
print("⬇️ Baixando CNAE Subclasses (Isso pode demorar uns 30 segundos)...")
url_cnae = "https://servicodados.ibge.gov.br/api/v2/cnae/subclasses"

try:
    response = requests.get(url_cnae)
    data_cnae = response.json()
    lista_cnae = []
    
    if isinstance(data_cnae, list):
        for item in data_cnae:
            try:
                classe = item.get('classe', {})
                grupo = classe.get('grupo', {})
                divisao = grupo.get('divisao', {})
                secao = divisao.get('secao', {})
                
                cnae_limpo = item['id'].replace("-", "").replace("/", "").replace(".", "")
                
                lista_cnae.append({
                    'subclasse_codigo': int(cnae_limpo),
                    'subclasse_descricao': item['descricao'],
                    'secao_codigo': secao.get('id', ''),
                    'secao_descricao': secao.get('descricao', '')
                })
            except Exception:
                continue
        
        df_cnae = pd.DataFrame(lista_cnae)
        path_cnae = os.path.join(REFS_DIR, 'cnae.csv')
        df_cnae.to_csv(path_cnae, sep=';', index=False)
        print(f"✅ Tabela 'cnae.csv' criada com {len(df_cnae)} atividades.")
    else:
        print(f"❌ Erro: API CNAE retornou formato inesperado.")
except Exception as e:
    print(f"❌ Erro crítico ao baixar CNAE: {e}")


# ==============================================================================
# 4. TABELAS FIXAS DO CAGED (Manuais)
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

print("✅ Setup concluído com sucesso!")