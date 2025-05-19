import streamlit as st
from dbfread import DBF
import pandas as pd
import os
import zipfile
from io import BytesIO
import numpy as np

# Configuração da página
st.set_page_config(page_title="Análise de SRAG - CRS 014", layout="wide")

# Título do aplicativo
st.title("Análise de Dados de SRAG - CRS 014")
st.markdown("""
Este painel permite analisar dados de SRAG (Síndrome Respiratória Aguda Grave) da região CRS 014.
Carregue arquivos ZIP contendo DBFs para iniciar a análise.
""")

# Upload de arquivos
uploaded_files = st.sidebar.file_uploader("Carregue arquivos ZIP com dados DBF", type="zip", accept_multiple_files=True)

if uploaded_files:
    # Processamento dos arquivos
    with st.spinner('Processando arquivos...'):
        dados_2020_2021_2022 = pd.DataFrame()
        dados_geral = pd.DataFrame()
        
        # Extrair arquivos ZIP
        for uploaded_file in uploaded_files:
            with zipfile.ZipFile(BytesIO(uploaded_file.read()), 'r') as zip_ref:
                zip_ref.extractall('temp_zip')
        
        # Processar arquivos DBF
        for arquivo_dbf in sorted(os.listdir('temp_zip/')):
            if arquivo_dbf.lower().endswith('.dbf'):
                arquivo_dbf_path = 'temp_zip/' + arquivo_dbf
                dbf = DBF(arquivo_dbf_path, encoding='latin-1', ignore_missing_memofile=True)
                dados = pd.DataFrame(iter(dbf))
                
                # Filtros específicos
                dados_filtrados = dados[(dados['CLASSI_FIN']=='5')&
                                      (dados['ID_RG_RESI']=='014 CRS')&
                                      (dados['UTI']=='1')&
                                      (dados['DT_SAIDUTI']=='')]
                
                dados_filtrados = dados_filtrados.filter(['NM_PACIENT', 'ID_MN_RESI','DT_NOTIFIC'])
                dados_filtrados.columns = ['nome do paciente', 'municipio de residencia', 'data da notificacao']
                dados_2020_2021_2022 = pd.concat([dados_2020_2021_2022, dados_filtrados])
                dados_geral = pd.concat([dados_geral, dados])
        
        # Limpar diretório temporário
        for f in os.listdir('temp_zip/'):
            os.remove(os.path.join('temp_zip/', f))
        os.rmdir('temp_zip/')
        
        # Processar dados consolidados
        dados_2020_2021_2022.reset_index(drop=True).sort_values(by='municipio de residencia').drop_duplicates(inplace=True)
        
        dados_consolidados = dados_geral[(dados_geral['CLASSI_FIN']=='5')&(dados_geral['ID_RG_RESI']=='014 CRS')]
        consolidado = pd.DataFrame(dados_consolidados.groupby(by='ID_MN_RESI')['ID_RG_RESI'].count()).reset_index()
        consolidado.columns = ['municipio de residencia', 'total']

        
        dados_consolidados3 = dados_geral[(dados_geral['ID_RG_RESI']=='014 CRS')]
        dados_consolidados2 = dados_consolidados3.filter(['NM_PACIENT', 'ID_MN_RESI',"DT_NOTIFIC", 'DT_SIN_PRI', 'CRITERIO',
                                                         'UTI', 'DT_SAIDUTI', 'CLASSI_FIN', 'EVOLUCAO', 'DT_EVOLUCAO' , 'PCR_RESUL' , 'TP_FLU_PCR' , 'PCR_FLUASU' , 'PCR_FLUBLI' , 'PCR_VSR', 'PCR_PARA1' , 'PCR_PARA2' ,
                                                          'PCR_PARA3' , 'PCR_PARA4' , 'PCR_ADENO' , 'PCR_RINO'])

        # Transformações de dados
        dados_consolidados2['criterio_texto'] = dados_consolidados2['CRITERIO'].replace({'1':'laboratorial',
                                                                                         '2':'clinico-epidemiologico',
                                                                                         '3':'clinico',
                                                                                         '4':'clinico-imagem'})

        dados_consolidados2['UTI_texto'] = dados_consolidados2['UTI'].replace({'1':'Sim', '2':'Não', '9':'Ignorado'})

        dados_consolidados2['Classificação final'] = dados_consolidados2['CLASSI_FIN'].replace({'1':'SRAG por influenza',
                                                                                         '2':'SRAG por outro vírus respiratório',
                                                                                         '3':'SRAG por outro agente etiológico',
                                                                                         '4':'SRAG não especificado',
                                                                                         '5':'SRAG por Covid-19',
                                                                                         '':'Suspeito'})

        dados_consolidados2['Evolução'] = dados_consolidados2['EVOLUCAO'].replace({'1':'Cura',
                                                                                         '2':'Óbito',
                                                                                         '3':'Óbito por outras causas',
                                                                                         '9':'Ignorado',
                                                                                         '':'Aguardando'})

        dados_consolidados2['PCR_RESUL'] = dados_consolidados2['PCR_RESUL'].replace({'1':'Detectável',
                                                                                          '2':'Não detectável',
                                                                                          '3':'Inconclusivo',
                                                                                          '4':'Não realizado',
                                                                                          '5':'Aguardando resultado',
                                                                                          '9':'Ignorado'})

        dados_consolidados2['TP_FLU_PCR'] = dados_consolidados2['TP_FLU_PCR'].replace({'1':'Influenza A',
                                                                                         '2':'Influenza B'})

        dados_consolidados2['PCR_FLUASU'] = dados_consolidados2['PCR_FLUASU'].replace({'1':'Influenza A (H1N1)',
                                                                                         '2':'Influenza A (H3N2)',
                                                                                         '3': 'Influenza nao subtipado',
                                                                                         '4': 'Influenza nao subtipavel',
                                                                                         '5': 'Inconclusivo',
                                                                                         '6': 'outro'})

        dados_consolidados2['PCR_FLUBLI'] = dados_consolidados2['PCR_FLUBLI'].replace({'1':'Victoria',
                                                                                         '2':'Yamagatha',
                                                                                         '3': 'Nao realizado',
                                                                                         '4': 'Inconclusivo',
                                                                                         '5': 'outro'})

        lista_virusresp = ['PCR_VSR', 'PCR_PARA1' , 'PCR_PARA2' , 'PCR_PARA3' , 'PCR_PARA4' , 'PCR_ADENO' , 'PCR_RINO']

        for i in lista_virusresp:
            dados_consolidados2[i] = dados_consolidados2[i].replace({'1': 'sim' , ' ' : ' '})

        dados_consolidados2 = dados_consolidados2[['NM_PACIENT', 'ID_MN_RESI', 'DT_NOTIFIC', 'DT_SIN_PRI', 'UTI_texto',
                                                   'DT_SAIDUTI', 'criterio_texto',  'Classificação final', 'Evolução' ,
                                                   'PCR_RESUL' , 'TP_FLU_PCR' , 'PCR_FLUASU' , 'PCR_FLUBLI' , 'PCR_VSR' ,
                                                   'PCR_PARA1' , 'PCR_PARA2' , 'PCR_PARA3' , 'PCR_PARA4' , 'PCR_ADENO' ,
                                                   'PCR_RINO']]

        dados_consolidados2.columns = ['nome', 'municipio de residencia', 'data de notificacao', 'inicio dos sintomas',
                                       'Foi para UTI?', 'Data de saída da UTI', 'Critério de confirmação',
                                       'Classificação final', 'Evolução' , 'Resultado outro PCR' ,
                                       'Tipo Influenza' ,'subtipo Influenza A' , 'subtipo Influenza B' , 'VSR' ,
                                       'PARA1' , 'PARA2' , 'PARA3' , 'PARA4' , 'ADENO' , 'RINO']

        dados_consolidados2['data de notificacao'] = pd.to_datetime(dados_consolidados2['data de notificacao'], dayfirst=True)
        dados_consolidados2 = dados_consolidados2.sort_values(by='data de notificacao')
        dados_consolidados2['data de notificacao'] = dados_consolidados2['data de notificacao'].astype('str')

        # Consolidado por vírus por município
        # Supondo que df seja seu DataFrame original
        df = dados_consolidados2.copy()

        # 1. Identificar colunas disponíveis
        colunas_base = ['municipio de residencia', 'Classificação final', 'Tipo Influenza', 
                        'subtipo Influenza A', 'subtipo Influenza B', 'VSR', 'ADENO', 'RINO']
        
        # Verificar colunas existentes
        colunas_disponiveis = [col for col in colunas_base if col in df.columns]
        print(f"Colunas disponíveis: {colunas_disponiveis}")
        
        # 2. Criar colunas para cada tipo de vírus
        # COVID
        df['COVID'] = df['Classificação final'].apply(lambda x: 1 if x == 'SRAG por Covid-19' else 0)
        
        # Influenza A e subtipos
        df['INFLUENZA_A'] = df['Tipo Influenza'].apply(lambda x: 1 if x == 'Influenza A' else 0)
        df['INFLUENZA_A_H1N1'] = df['subtipo Influenza A'].apply(lambda x: 1 if x == 'Influenza A (H1N1)' else 0)
        df['INFLUENZA_A_H3N2'] = df['subtipo Influenza A'].apply(lambda x: 1 if x == 'Influenza A (H3N2)' else 0)
        
        # Influenza B e subtipos
        df['INFLUENZA_B'] = df['Tipo Influenza'].apply(lambda x: 1 if x == 'Influenza B' else 0)
        df['INFLUENZA_B_VICTORIA'] = df['subtipo Influenza B'].apply(lambda x: 1 if x == 'Victoria' else 0)
        df['INFLUENZA_B_YAMAGATA'] = df['subtipo Influenza B'].apply(lambda x: 1 if x == 'Yamagatha' else 0)
        
        # Outros vírus respiratórios
        virus_cols = ['VSR', 'ADENO', 'RINO']
        for col in virus_cols:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: 1 if str(x).strip().lower() in ['sim', '1'] else 0)
        
        # 3. Criar a tabela consolidada
        cols_contagem = ['COVID', 'INFLUENZA_A', 'INFLUENZA_A_H1N1', 'INFLUENZA_A_H3N2',
                        'INFLUENZA_B', 'INFLUENZA_B_VICTORIA', 'INFLUENZA_B_YAMAGATA'] + virus_cols
        
        tabela_virus = df.groupby('municipio de residencia')[cols_contagem].sum()
        
        # 4. Adicionar totais
        #tabela_virus['TOTAL_INFLUENZA'] = tabela_virus['INFLUENZA_A'] + tabela_virus['INFLUENZA_B']
        tabela_virus['TOTAL_VIRUS'] = tabela_virus.sum(axis=1)
        
        # 5. Reordenar colunas
        ordem_colunas = ['COVID', 
                        'INFLUENZA_A', 'INFLUENZA_A_H1N1', 'INFLUENZA_A_H3N2',
                        'INFLUENZA_B', 'INFLUENZA_B_VICTORIA', 'INFLUENZA_B_YAMAGATA',
                        ] + virus_cols + ['TOTAL_VIRUS']
        
        tabela_virus = tabela_virus[ordem_colunas]
        
        # 6. Resetar índice e formatar
        tabela_virus = tabela_virus.reset_index()
        tabela_virus = tabela_virus.rename(columns={'municipio de residencia': 'Município'})

        # Obitos
        obitos = pd.pivot_table(dados_consolidados2[dados_consolidados2['Evolução']=='Óbito'], index=['municipio de residencia'], columns = ['Classificação final'], aggfunc='size').fillna(0)

        

    # Visualização dos dados
    st.success('Processamento concluído!')
    
    # Abas para diferentes visualizações
    tab1, tab2, tab3 = st.tabs(["Pacientes em UTI", "Casos por Município", "Dados Detalhados"])
    
    with tab1:
        st.header("Pacientes com COVID-19 em UTI (sem data de saída)")
        st.dataframe(dados_2020_2021_2022)
        
        # Opção para download
        csv = dados_2020_2021_2022.to_csv(index=False).encode('utf-8')
        st.download_button(
            "Baixar dados como CSV",
            csv,
            "pacientes_uti.csv",
            "text/csv",
            key='download-csv'
        )
    
    with tab2:
        st.header("Total de casos por município")
        
        # Função para aplicar cores
        def color_cells(val):
            if val == 0:
                color = '#F5F5F5'  # Cinza claro
            elif 1 <= val <= 3:
                color = '#FFF3CD'  # Amarelo claro (alerta)
            elif 4 <= val <= 6:
                color = '#FFE082'  # Amarelo médio
            elif 7 <= val <= 9:
                color = '#FFA000'  # Amarelo escuro
            else:
                color = '#D32F2F'  # Vermelho (crítico)
            return f'background-color: {color}; color: black;'
        
        # Título
        #st.title('Distribuição de Vírus por Município')
        st.markdown("""
        <style>
            .stDataFrame div[data-testid="stDataFrameContainer"] {
                width: 100% !important;
            }
        </style>
        """, unsafe_allow_html=True)
        
        # Aplicar estilo
        styled_df = (
            tabela_virus.set_index('Município').style
            .applymap(color_cells, subset=pd.IndexSlice[:, tabela_virus.columns[1:]])
            .format("{:.0f}", na_rep="-")
            .set_properties(**{'text-align': 'center'})
            .set_table_styles([
                {'selector': 'th', 'props': [('background-color', '#2c3e50'), ('color', 'white')]},
                {'selector': 'td:hover', 'props': [('background-color', '#bdc3c7')]}
            ])
        )
        
        # Mostrar tabela
        st.dataframe(styled_df, use_container_width=True, height = 700)
        
        # Legenda de cores
        #st.markdown("""
        #**Legenda:**
        #- <span style='background-color:#F5F5F5; padding: 2px 5px; border-radius: 3px;'>0 casos</span>
        #- <span style='background-color:#FFF3CD; padding: 2px 5px; border-radius: 3px;'>1-3 casos</span>
        #- <span style='background-color:#FFE082; padding: 2px 5px; border-radius: 3px;'>4-6 casos</span>
        #- <span style='background-color:#FFA000; padding: 2px 5px; border-radius: 3px;'>7-9 casos</span>
        #- <span style='background-color:#D32F2F; color:white; padding: 2px 5px; border-radius: 3px;'>10+ casos</span>
        #""", unsafe_allow_html=True)
               

        st.header("Total de óbitos por município")
        st.dataframe(obitos)
        
        # Gráfico de barras
        #st.bar_chart(tabela_virus, stack=False)
    
    with tab3:
        st.header("Dados detalhados de todos os casos")
        st.dataframe(dados_consolidados2)
        
        
        # Filtros interativos
        st.subheader("Filtrar dados")
        col1, col2 = st.columns(2)
        
        with col1:
            municipio = st.selectbox(
                'Município',
                options=['Todos'] + sorted(dados_consolidados2['municipio de residencia'].unique().tolist()))
            
            classificacao = st.selectbox(
                'Classificação Final',
                options=['Todos'] + sorted(dados_consolidados2['Classificação final'].unique().tolist()))
        
        with col2:
            evolucao = st.selectbox(
                'Evolução',
                options=['Todos'] + sorted(dados_consolidados2['Evolução'].unique().tolist()))
            
            uti = st.selectbox(
                'UTI',
                options=['Todos'] + sorted(dados_consolidados2['Foi para UTI?'].unique().tolist()))
        
        # Aplicar filtros
        filtered_data = dados_consolidados2.copy()
        if municipio != 'Todos':
            filtered_data = filtered_data[filtered_data['municipio de residencia'] == municipio]
        if classificacao != 'Todos':
            filtered_data = filtered_data[filtered_data['Classificação final'] == classificacao]
        if evolucao != 'Todos':
            filtered_data = filtered_data[filtered_data['Evolução'] == evolucao]
        if uti != 'Todos':
            filtered_data = filtered_data[filtered_data['Foi para UTI?'] == uti]
        
        st.dataframe(filtered_data)
        
        # Opção para download
        csv = filtered_data.to_csv(index=False).encode('utf-8')
        st.download_button(
            "Baixar dados filtrados como CSV",
            csv,
            "dados_filtrados.csv",
            "text/csv",
            key='download-filtered-csv'
        )
else:
    st.warning("Por favor, carregue os arquivos ZIP para começar a análise.")
