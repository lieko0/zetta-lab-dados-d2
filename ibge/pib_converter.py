import pandas as pd
import os
import warnings

def converter_dados_pib_para(arquivo_entrada=None, arquivo_saida="pib_para_estudo.csv"):
    """
    Converter dados do PIB do IBGE para CSV, filtrando apenas dados do estado do Pará.
    Adaptado para projeto "Avaliação e Previsão dos Impactos Socioeconômicos do Desmatamento no Estado do Pará".
    
    Parâmetros:
    arquivo_entrada (str): Caminho para o arquivo de entrada (Excel, CSV ou TSV)
    arquivo_saida (str): Nome do arquivo CSV de saída
    """
    # Suprimir warnings específicos
    warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
    
    # Se não foi fornecido arquivo de entrada, procurar por arquivos no diretório atual
    if arquivo_entrada is None:
        # Verificar possíveis arquivos de entrada (por ordem de prioridade)
        possiveis_arquivos = [
            f for f in os.listdir('.') 
            if f.endswith(('.xlsx', '.xls', '.csv', '.tsv')) and 'pib' in f.lower()
        ]
        
        if not possiveis_arquivos:
            print("Erro: Nenhum arquivo de dados PIB encontrado no diretório atual.")
            print("Por favor, especifique o caminho do arquivo de entrada.")
            return
        
        arquivo_entrada = possiveis_arquivos[0]
        print(f"Usando arquivo encontrado automaticamente: {arquivo_entrada}")
    
    print(f"Processando arquivo: {arquivo_entrada}")
    
    try:
        # Detectar formato do arquivo baseado na extensão
        extensao = os.path.splitext(arquivo_entrada)[1].lower()
        
        if extensao in ['.xlsx', '.xls']:
            # Ler arquivo Excel
            print("Detectado arquivo Excel.")
            try:
                # Tentar ler primeiro usando engine openpyxl (para .xlsx)
                df = pd.read_excel(arquivo_entrada, engine='openpyxl')
            except:
                # Se falhar, tentar com engine xlrd (para .xls)
                df = pd.read_excel(arquivo_entrada, engine='xlrd')
        elif extensao == '.csv':
            # Tentar diferentes encodings e delimitadores para CSV
            try:
                df = pd.read_csv(arquivo_entrada, encoding='utf-8')
            except:
                try:
                    df = pd.read_csv(arquivo_entrada, encoding='latin1')
                except:
                    # Tentar com delimiter específico (ponto e vírgula comum em CSVs brasileiros)
                    df = pd.read_csv(arquivo_entrada, encoding='utf-8', sep=';')
        elif extensao == '.tsv':
            # Ler arquivo TSV
            df = pd.read_csv(arquivo_entrada, sep='\t', encoding='utf-8')
        else:
            print(f"Erro: Formato de arquivo não suportado - {extensao}")
            return
        
        # Mostrar informações básicas
        print(f"Total de linhas no arquivo original: {len(df)}")
        print(f"Colunas disponíveis: {', '.join(df.columns[:5])}...")
        
        # Verificar se o DataFrame tem a coluna necessária para filtrar
        coluna_uf = None
        for possivel_coluna in [
            'Sigla da Unidade da Federação', 'UF', 'Estado', 'uf', 'estado', 
            'SIGLA_UF', 'SG_UF', 'SG_ESTADO'
        ]:
            if possivel_coluna in df.columns:
                coluna_uf = possivel_coluna
                break
        
        if coluna_uf is None:
            print("Aviso: Não foi possível identificar a coluna de UF.")
            print("Colunas disponíveis:")
            for col in df.columns:
                print(f"  - {col}")
            
            coluna_uf = input("Digite o nome exato da coluna que contém a sigla da UF: ")
            if coluna_uf not in df.columns:
                print(f"Erro: Coluna '{coluna_uf}' não encontrada. Encerrando.")
                return
        
        # Filtrar apenas os dados do estado do Pará
        df_para = df[df[coluna_uf] == 'PA']
        
        print(f"Total de linhas após filtro do Pará: {len(df_para)}")
        
        # Verificar se há dados
        if len(df_para) == 0:
            print("Nenhum dado encontrado para o estado do Pará!")
            return
        
        # Mapear colunas do arquivo para colunas padrão
        # Primeiro, verificar quais colunas do formato padrão estão presentes
        colunas_padrao = {
            'Ano': ['Ano', 'ANO', 'ano'],
            'Código do Município': ['Código do Município', 'codigo_municipio', 'COD_MUN', 'cd_mun', 'CODIGO_IBGE'],
            'Nome do Município': ['Nome do Município', 'municipio', 'NOME_MUNICIPIO', 'NM_MUN'],
            'Código da Microrregião': ['Código da Microrregião', 'codigo_microrregiao', 'COD_MICRO'],
            'Nome da Microrregião': ['Nome da Microrregião', 'microrregiao', 'NOME_MICRO'],
            'Amazônia Legal': ['Amazônia Legal', 'amazonia_legal', 'AMAZONIA_LEGAL', 'AM_LEGAL'],
            'Valor adicionado bruto da Agropecuária,  a preços correntes (R$ 1.000)': [
                'Valor adicionado bruto da Agropecuária,  a preços correntes (R$ 1.000)',
                'Valor adicionado bruto da Agropecuária, a preços correntes (R$ 1.000)',
                'valor_agropecuaria', 'VA_AGRO', 'PIB_AGRO'
            ],
            'Valor adicionado bruto da Indústria, a preços correntes (R$ 1.000)': [
                'Valor adicionado bruto da Indústria, a preços correntes (R$ 1.000)', 
                'valor_industria', 'VA_IND', 'PIB_IND'
            ],
            'Valor adicionado bruto dos Serviços, a preços correntes  - exceto Administração, defesa, educação e saúde públicas e seguridade social (R$ 1.000)': [
                'Valor adicionado bruto dos Serviços, a preços correntes  - exceto Administração, defesa, educação e saúde públicas e seguridade social (R$ 1.000)',
                'valor_servicos', 'VA_SERV', 'PIB_SERV'
            ],
            'Valor adicionado bruto da Administração, defesa, educação e saúde públicas e seguridade social,  a preços correntes (R$ 1.000)': [
                'Valor adicionado bruto da Administração, defesa, educação e saúde públicas e seguridade social,  a preços correntes (R$ 1.000)',
                'valor_administracao_publica', 'VA_ADM', 'PIB_ADM'
            ],
            'Valor adicionado bruto total,  a preços correntes (R$ 1.000)': [
                'Valor adicionado bruto total,  a preços correntes (R$ 1.000)',
                'valor_total', 'VA_TOTAL', 'PIB_VA_TOTAL'
            ],
            'Impostos, líquidos de subsídios, sobre produtos,  a preços correntes (R$ 1.000)': [
                'Impostos, líquidos de subsídios, sobre produtos,  a preços correntes (R$ 1.000)',
                'impostos', 'IMPOSTOS', 'IMP_LIQ'
            ],
            'Produto Interno Bruto,  a preços correntes (R$ 1.000)': [
                'Produto Interno Bruto,  a preços correntes (R$ 1.000)',
                'pib', 'PIB', 'PIB_TOTAL'
            ],
            'Produto Interno Bruto per capita,  a preços correntes (R$ 1,00)': [
                'Produto Interno Bruto per capita,  a preços correntes (R$ 1,00)',
                'pib_per_capita', 'PIB_PERCAPITA', 'PIB_PC'
            ],
            'Atividade com maior valor adicionado bruto': [
                'Atividade com maior valor adicionado bruto',
                'principal_atividade', 'ATIV_PRINCIPAL'
            ],
            'Atividade com segundo maior valor adicionado bruto': [
                'Atividade com segundo maior valor adicionado bruto',
                'segunda_atividade', 'ATIV_SECUNDARIA'
            ],
            'Atividade com terceiro maior valor adicionado bruto': [
                'Atividade com terceiro maior valor adicionado bruto',
                'terceira_atividade', 'ATIV_TERCIARIA'
            ]
        }
        
        # Criar mapeamento de colunas encontradas para nomes padronizados
        mapeamento = {}
        colunas_encontradas = []
        
        for coluna_padrao, alternativas in colunas_padrao.items():
            coluna_encontrada = None
            for alt in alternativas:
                if alt in df_para.columns:
                    coluna_encontrada = alt
                    break
            
            if coluna_encontrada:
                colunas_encontradas.append(coluna_encontrada)
                # Se a coluna encontrada já não estiver no formato padrão final
                if coluna_encontrada != alternativas[1]:  # alternativas[1] é o nome limpo padronizado
                    mapeamento[coluna_encontrada] = alternativas[1]
        
        # Selecionar apenas colunas encontradas
        df_para_limpo = df_para[colunas_encontradas].copy()
        
        # Renomear colunas se necessário
        if mapeamento:
            df_para_limpo = df_para_limpo.rename(columns=mapeamento)
        
        # Lista das colunas após renomeação
        colunas_finais = [mapeamento.get(col, col) for col in colunas_encontradas]
        
        # Converter valores monetários para tipo numérico
        colunas_numericas = [
            'valor_agropecuaria', 'valor_industria', 'valor_servicos', 
            'valor_administracao_publica', 'valor_total', 'impostos', 'pib', 'pib_per_capita'
        ]
        
        for coluna in colunas_numericas:
            if coluna in df_para_limpo.columns:
                # Remover espaços e substituir vírgula por ponto se necessário
                df_para_limpo[coluna] = df_para_limpo[coluna].astype(str).str.strip().str.replace(',', '.')
                # Remover possíveis caracteres não numéricos (R$, etc)
                df_para_limpo[coluna] = df_para_limpo[coluna].str.replace(r'[^\d.-]', '', regex=True)
                # Converter para float
                df_para_limpo[coluna] = pd.to_numeric(df_para_limpo[coluna], errors='coerce')
        
        # Adicionar coluna com o ano de criação do município (se disponível)
        try:
            # Tentar carregar dados de ano de criação se existir um arquivo para isso
            arquivo_anos = "municipios_criacao.csv"
            if os.path.exists(arquivo_anos):
                df_anos = pd.read_csv(arquivo_anos)
                
                # Verificar qual é a coluna de código do município nos dados carregados
                col_codigo = 'codigo_municipio' if 'codigo_municipio' in df_para_limpo.columns else None
                
                if col_codigo:
                    df_para_limpo = pd.merge(
                        df_para_limpo, 
                        df_anos[['codigo_municipio', 'ano_criacao']], 
                        on='codigo_municipio', 
                        how='left'
                    )
                    # Criar coluna de classificação conforme mencionado no relatório
                    df_para_limpo['municipio_antigo'] = df_para_limpo['ano_criacao'].apply(
                        lambda x: 'Sim' if x <= 1970 else 'Não' if pd.notna(x) else None
                    )
        except Exception as e:
            print(f"Nota: Não foi possível adicionar dados de ano de criação: {e}")
            print("Você precisará adicionar essa informação manualmente ou criar um arquivo 'municipios_criacao.csv'")
            
        # Calcular proporção da agropecuária no PIB (importante para análise do desmatamento)
        if 'valor_agropecuaria' in df_para_limpo.columns and 'valor_total' in df_para_limpo.columns:
            df_para_limpo['proporcao_agropecuaria'] = (df_para_limpo['valor_agropecuaria'] / 
                                                    df_para_limpo['valor_total'] * 100)
        
        # Salvar como CSV
        df_para_limpo.to_csv(arquivo_saida, index=False, encoding='utf-8')
        
        print(f"Arquivo CSV do Pará criado com sucesso: {arquivo_saida}")
        print(f"Total de {len(df_para_limpo)} municípios do Pará salvos no arquivo.")
        print(f"Colunas disponíveis para análise: {', '.join(df_para_limpo.columns)}")
        
        return df_para_limpo
    
    except Exception as e:
        print(f"Erro ao processar o arquivo: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Você pode chamar a função sem argumentos para busca automática de arquivos
    # converter_dados_pib_para()
    
    # Ou especificar o arquivo de entrada e saída
    converter_dados_pib_para(
        arquivo_entrada="data/raw/ibge_pib_municipios_adapted.xlsx",  # Substitua pelo nome do seu arquivo Excel
        arquivo_saida="data/processed/pib_para_estudo.csv"
    )