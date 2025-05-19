import streamlit as st
from dbfread import DBF
import pandas as pd
import os
import zipfile
from io import BytesIO

# Configuração da página
st.set_page_config(page_title="Análise de SRAG - CRS 014", layout="wide")

# Título do aplicativo
st.title("Análise de Dados de SRAG - CRS 014")
st.markdown("""
Este painel permite analisar dados de SRAG (Síndrome Respiratória Aguda Grave) da região CRS 014.
Carregue arquivos ZIP contendo DBFs para iniciar a análise.
""")

# Upload de arquivos
uploaded_files = st.file_uploader("Carregue arquivos ZIP com dados DBF", type="zip", accept_multiple_files=True)

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
        
        # Criar tabela consolidada por tipo de vírus e óbitos
        dados_consolidados = dados_geral[(dados_geral['ID_RG_RESI']=='014 CRS')].copy()
        
        # Classificar os casos por tipo de vírus
        dados_consolidados['Tipo_Virus'] = 'OUTROS'
        dados_consolidados.loc[dados_consolidados['CLASSI_FIN'] == '5', 'Tipo_Virus'] = 'COVID'
        dados_consolidados.loc[dados_consolidados['CLASSI_FIN'] == '1', 'Tipo_Virus'] = 'INFLUENZA'
        dados_consolidados.loc[dados_consolidados['PCR_VSR'] == '1', 'Tipo_Virus'] = 'VSR'
        
        # Identificar óbitos
        dados_consolidados['OBITO'] = (dados_consolidados['EVOLUCAO'] == '2').astype(int)
        
        # Criar tabela consolidada
        casos = dados_consolidados.groupby(['ID_MN_RESI', 'Tipo_Virus']).size().unstack(fill_value=0)
        obitos = dados_consolidados.groupby(['ID_MN_RESI', 'Tipo_Virus'])['OBITO'].sum().unstack(fill_value=0)
        
        # Juntar casos e óbitos em uma única tabela
        consolidado = pd.concat([
            casos.add_prefix('Casos_'), 
            obitos.add_prefix('Obitos_')
        ], axis=1)
        
        # Preencher valores ausentes com 0
        for col in ['Casos_COVID', 'Casos_INFLUENZA', 'Casos_VSR', 'Obitos_COVID', 'Obitos_INFLUENZA', 'Obitos_VSR']:
            if col not in consolidado.columns:
                consolidado[col] = 0
        
        # Reordenar colunas
        consolidado = consolidado[['Casos_COVID', 'Casos_INFLUENZA', 'Casos_VSR', 'Obitos_COVID', 'Obitos_INFLUENZA', 'Obitos_VSR']]
        consolidado = consolidado.reset_index()
        consolidado.columns = ['Município', 'Casos COVID', 'Casos INFLUENZA', 'Casos VSR', 
                              'Óbitos COVID', 'Óbitos INFLUENZA', 'Óbitos VSR']
        
        # Ordenar por município
        consolidado = consolidado.sort_values('Município')
        
        # Processar dados detalhados (mantido do código original)
        dados_consolidados2 = dados_consolidados3.filter(['NM_PACIENT', 'ID_MN_RESI',"DT_NOTIFIC", 'DT_SIN_PRI', 'CRITERIO',
                                                         'UTI', 'DT_SAIDUTI', 'CLASSI_FIN', 'EVOLUCAO', 'DT_EVOLUCAO' , 'PCR_RESUL' , 'TP_FLU_PCR' , 'PCR_FLUASU' , 'PCR_FLUBLI' , 'PCR_VSR', 'PCR_PARA1' , 'PCR_PARA2' ,
                                                          'PCR_PARA3' , 'PCR_PARA4' , 'PCR_ADENO' , 'PCR_RINO'])

        # ... (restante das transformações de dados mantido igual)

    # Visualização dos dados
    st.success('Processamento concluído!')
    
    # Abas para diferentes visualizações
    tab1, tab2, tab3 = st.tabs(["Pacientes em UTI", "Casos por Município", "Dados Detalhados"])
    
    with tab1:
        st.header("Pacientes com COVID-19 em UTI (sem data de saída)")
        st.dataframe(dados_2020_2021_2022)
        
        csv = dados_2020_2021_2022.to_csv(index=False).encode('utf-8')
        st.download_button(
            "Baixar dados como CSV",
            csv,
            "pacientes_uti.csv",
            "text/csv",
            key='download-csv'
        )
    
    with tab2:
        st.header("Casos e Óbitos por Município e Tipo de Vírus")
        
        # Mostrar tabela consolidada
        st.dataframe(consolidado)
        
        # Gráfico de barras empilhadas
        st.subheader("Distribuição de Casos por Tipo de Vírus")
        
        # Preparar dados para o gráfico
        casos_grafico = consolidado.set_index('Município')[['Casos COVID', 'Casos INFLUENZA', 'Casos VSR']]
        
        # Plotar gráfico de barras
        st.bar_chart(casos_grafico)
        
        # Gráfico de óbitos
        st.subheader("Distribuição de Óbitos por Tipo de Vírus")
        obitos_grafico = consolidado.set_index('Município')[['Óbitos COVID', 'Óbitos INFLUENZA', 'Óbitos VSR']]
        st.bar_chart(obitos_grafico)
        
        # Opção para download
        csv = consolidado.to_csv(index=False).encode('utf-8')
        st.download_button(
            "Baixar dados consolidados como CSV",
            csv,
            "casos_obitos_municipio.csv",
            "text/csv",
            key='download-consolidado-csv'
        )
    
    with tab3:
        st.header("Dados detalhados de todos os casos")
        st.dataframe(dados_consolidados2)
        
        # Filtros interativos (mantido igual)
        # ...

else:
    st.warning("Por favor, carregue os arquivos ZIP para começar a análise.")
