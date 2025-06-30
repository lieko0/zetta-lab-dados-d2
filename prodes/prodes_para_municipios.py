import geopandas as gpd
import pandas as pd
import os
from shapely.geometry import MultiPolygon

def processar_prodes_para(input_shapefile, output_csv):
    """
    Processa dados PRODES da Amazônia Legal, filtrando apenas para o estado do Pará
    e organizando por município.
    
    Args:
        input_shapefile: Caminho para o arquivo shapefile do PRODES
        output_csv: Caminho para salvar o arquivo CSV de saída
    """
    print("Carregando o shapefile do PRODES...")
    # Carregar o shapefile do PRODES
    try:
        prodes_gdf = gpd.read_file(input_shapefile)
        print(f"Shapefile carregado com sucesso. Total de registros: {len(prodes_gdf)}")
    except Exception as e:
        print(f"Erro ao carregar o shapefile: {str(e)}")
        return
    
    # Verificar se o shapefile tem a coluna 'state'
    if 'state' not in prodes_gdf.columns:
        print("Coluna 'state' não encontrada no shapefile.")
        print("Colunas disponíveis:", prodes_gdf.columns.tolist())
        return
    
    # Filtrar apenas para o estado do Pará
    print("Filtrando dados apenas para o estado do Pará...")
    para_gdf = prodes_gdf[prodes_gdf['state'] == 'PA']
    print(f"Dados filtrados para o Pará. Registros selecionados: {len(para_gdf)}")
    
    if len(para_gdf) == 0:
        print("Nenhum dado encontrado para o estado do Pará.")
        print("Valores únicos na coluna 'state':", prodes_gdf['state'].unique())
        return
    
    # Carregar shapefile dos municípios do Brasil
    # Você precisa ter este arquivo. Caso não tenha, pode baixá-lo do IBGE
    try:
        print("Carregando shapefile dos municípios do Brasil...")
        # Altere o caminho abaixo para o local onde está o shapefile dos municípios
        municipios_path = "municipios_ibge/BR_Municipios_2022.shp"  # Substitua pelo caminho correto
        municipios_gdf = gpd.read_file(municipios_path)
        
        # Filtrar apenas municípios do Pará
        municipios_para = municipios_gdf[municipios_gdf['SIGLA_UF'] == 'PA']
        print(f"Municípios do Pará carregados: {len(municipios_para)}")
    except Exception as e:
        print(f"Erro ao carregar o shapefile dos municípios: {str(e)}")
        print("Continuando sem a integração com municípios...")
        municipios_para = None
    
    # Verificar se há dados de anos disponíveis
    if 'year' not in para_gdf.columns:
        print("Coluna 'year' não encontrada. Tentando extrair do 'class_name'...")
        
        if 'class_name' in para_gdf.columns:
            # Extrair o ano do campo class_name (formato: "dYYYY")
            para_gdf['year'] = para_gdf['class_name'].str.extract(r'd(\d{4})').astype(int)
            print("Anos extraídos do campo 'class_name'.")
        else:
            print("Não foi possível identificar o ano do desmatamento.")
            return
    
    # Filtrar apenas dados a partir de 2008
    para_gdf = para_gdf[para_gdf['year'] >= 2008]
    print(f"Dados filtrados a partir de 2008. Registros: {len(para_gdf)}")
    
    # Se tiver os dados de municípios, fazer a intersecção espacial
    if municipios_para is not None:
        print("Realizando intersecção espacial com os municípios...")
        # Garantir que os sistemas de coordenadas sejam iguais
        if para_gdf.crs != municipios_para.crs:
            print(f"Reprojetando dados. CRS original: {para_gdf.crs}")
            para_gdf = para_gdf.to_crs(municipios_para.crs)
        
        # Realizar a intersecção espacial
        try:
            intersec_gdf = gpd.sjoin(para_gdf, municipios_para, how='inner', predicate='intersects')
            print(f"Intersecção concluída. Registros resultantes: {len(intersec_gdf)}")
            
            # Calcular a área de cada polígono em km²
            if 'area_km' not in intersec_gdf.columns:
                print("Calculando área em km²...")
                # Certifique-se de que o CRS esteja em uma projeção que preserva área
                temp_gdf = intersec_gdf.to_crs(epsg=5880)  # Projeção SIRGAS 2000 / Brazil Polyconic
                intersec_gdf['area_km'] = temp_gdf.geometry.area / 1_000_000  # Converter de m² para km²
            
            # Agrupar por município e ano
            print("Agrupando dados por município e ano...")
            
            # Nome da coluna do município pode variar dependendo do shapefile
            # Comum em shapefiles do IBGE: NM_MUN, NOME, NM_MUNICIP
            municipio_col = None
            for col in ['NM_MUN', 'NOME', 'NM_MUNICIP', 'NOM_MUN']:
                if col in intersec_gdf.columns:
                    municipio_col = col
                    break
            
            if municipio_col is None:
                print("Coluna do nome do município não encontrada.")
                print("Colunas disponíveis:", intersec_gdf.columns.tolist())
                # Usar código do município como fallback
                for col in ['CD_MUN', 'COD_MUN', 'GEOCODIGO']:
                    if col in intersec_gdf.columns:
                        municipio_col = col
                        break
            
            if municipio_col is not None:
                # Agrupar por município e ano, somando as áreas
                resultado = intersec_gdf.groupby([municipio_col, 'year'])['area_km'].sum().reset_index()
                print("Agrupamento concluído.")
            else:
                print("Não foi possível encontrar uma coluna de identificação do município.")
                return
        except Exception as e:
            print(f"Erro durante a intersecção espacial: {str(e)}")
            # Fallback: usar apenas os dados do PRODES sem intersecção
            resultado = para_gdf.groupby('year')['area_km'].sum().reset_index()
            resultado['municipio'] = 'DESCONHECIDO'
    else:
        # Se não tiver shapefile dos municípios, agrupa só por ano
        print("Agrupando apenas por ano (shapefile de municípios não disponível)...")
        resultado = para_gdf.groupby('year')['area_km'].sum().reset_index()
        resultado['municipio'] = 'DESCONHECIDO'
    
    # Salvar como CSV
    print(f"Salvando resultado em {output_csv}...")
    resultado.to_csv(output_csv, index=False, encoding='utf-8')
    print("Processamento concluído com sucesso!")
    return resultado

def baixar_shapefile_municipios():
    """
    Baixa o shapefile de municípios do IBGE, caso não esteja disponível.
    """
    import requests
    import zipfile
    import io
    
    print("Baixando shapefile de municípios do IBGE...")
    
    url = "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2022/Brasil/BR/BR_Municipios_2022.zip"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        # Extrair o arquivo zip
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
            zip_ref.extractall("municipios_ibge")
        
        print("Shapefile de municípios baixado e extraído com sucesso em 'municipios_ibge'.")
        return "municipios_ibge/BR_Municipios_2022.shp"
    except Exception as e:
        print(f"Erro ao baixar o shapefile de municípios: {str(e)}")
        return None

if __name__ == "__main__":
    # Caminho para o shapefile do PRODES
    input_shapefile = input("Digite o caminho para o shapefile do PRODES: ")
    
    # Verificar se o arquivo existe
    if not os.path.exists(input_shapefile):
        print(f"Arquivo {input_shapefile} não encontrado.")
        exit(1)
    
    # Verificar se temos o shapefile de municípios
    municipios_path = "municipios_ibge/BR_Municipios_2022.shp"
    if not os.path.exists(municipios_path):
        print("Shapefile de municípios não encontrado localmente.")
        opcao = input("Deseja baixar o shapefile de municípios do IBGE? (s/n): ")
        if opcao.lower() == 's':
            municipios_path = baixar_shapefile_municipios()
        else:
            print("Continuando sem o shapefile de municípios...")
    
    # Definir o caminho para o arquivo CSV de saída
    output_csv = "desmatamento_prodes_para_municipios_2008_2024.csv"
    
    # Processar os dados
    processar_prodes_para(input_shapefile, output_csv)
