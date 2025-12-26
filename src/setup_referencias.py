import os
import requests
import pandas as pd
import time

# Configuração de Pastas
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Garante que aponta para extracao_caged/data/refs, mesmo rodando de dentro de src
if os.path.basename(BASE_DIR) == 'src':
    BASE_DIR = os.path.dirname(BASE_DIR)

REFS_DIR = os.path.join(BASE_DIR, 'data', 'refs')

if not os.path.exists(REFS_DIR):
    os.makedirs(REFS_DIR)

print("🚀 Iniciando download e criação de tabelas de referência (Versão Corrigida)...")

# ==============================================================================
# 1. TABELA DE MUNICÍPIOS E UF (Fonte: API IBGE V1)
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
                # Navegação segura para evitar erro 'NoneType'
                micro = item.get('microrregiao', {})
                meso = micro.get('mesorregiao', {}) if micro else {}
                uf = meso.get('UF', {}) if meso else {}
                
                # Fallback: Se a estrutura hierárquica falhar, tenta pegar UF direto (alguns endpoints retornam diferente)
                if not uf and 'microrregiao' in item and 'UF' in item['microrregiao']: 
                     # Tentativa alternativa de estrutura
                     uf = item['microrregiao']['UF']

                if uf:
                    lista_mun.append({
                        'municipio_codigo': int(item['id']),
                        'municipio_nome': item['nome'],
                        'uf_codigo': int(uf.get('id', 0)),
                        'uf_sigla': uf.get('sigla', '')
                    })
            except Exception as e_item:
                print(f"⚠️ Aviso: Falha ao ler município {item.get('nome', '?')}: {e_item}")
                continue

        # Criação do DataFrame
        df_mun = pd.DataFrame(lista_mun)
        
        # Cria coluna de 6 dígitos (sem dígito verificador) para facilitar Joins
        df_mun['municipio_codigo_6'] = df_mun['municipio_codigo'].astype(str).str.slice(0, 6).astype(int)
        
        # Filtra apenas os válidos (UF > 0)
        df_mun = df_mun[df_mun['uf_codigo'] > 0]
        
        path_mun = os.path.join(REFS_DIR, 'municipios.csv')
        df_mun.to_csv(path_mun, sep=';', index=False)
        print(f"✅ Tabela 'municipios.csv' criada com {len(df_mun)} cidades.")
    else:
        print("❌ Erro: API do IBGE não retornou uma lista.")

except Exception as e:
    print(f"❌ Erro crítico ao baixar Municípios: {e}")

# ==============================================================================
# 2. TABELA CNAE (Subclasses e Seções) (Fonte: API IBGE V2)
# ==============================================================================
print("⬇️ Baixando CNAE Subclasses (Isso pode demorar uns 30 segundos)...")
# Nota: Baixar TODAS as subclasses é pesado. Vamos tentar a lista completa.
url_cnae = "https://servicodados.ibge.gov.br/api/v2/cnae/subclasses"

try:
    response = requests.get(url_cnae)
    data_cnae = response.json()
    
    lista_cnae = []
    
    if isinstance(data_cnae, list):
        for item in data_cnae:
            try:
                # Estrutura V2 é profunda: Classe -> Grupo -> Divisão -> Seção
                classe = item.get('classe', {})
                grupo = classe.get('grupo', {})
                divisao = grupo.get('divisao', {})
                secao = divisao.get('secao', {})
                
                cnae_limpo = item['id'].replace("-", "").replace("/", "").replace(".", "")
                
                lista_cnae.append({
                    'subclasse_codigo': int(cnae_limpo),
                    'subclasse_descricao': item['descricao'],
                    'secao_codigo': secao.get('id', ''),          # A, B, C...
                    'secao_descricao': secao.get('descricao', '') # Agricultura, Indústria...
                })
            except Exception as e_cnae:
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
# 3. TABELAS FIXAS DO CAGED (Manuais)
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

# Categoria (Tipo de emprego)
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

# Tipo Movimentação (Expandido)
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

print("✅ Setup concluído! Tabelas geradas em 'data/refs'.")