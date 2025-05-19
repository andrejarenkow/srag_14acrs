import streamlit as st
from dbfread import DBF
import pandas as pd
import zipfile
import os
import tempfile
import shutil

st.set_page_config(layout="wide")
st.title("Painel de Análise de SRAG - 014 CRS")

st.markdown("Faça upload de um ou mais arquivos `.zip` contendo arquivos `.dbf` para análise.")

uploaded_files = st.file_uploader("Upload dos arquivos ZIP", type="zip", accept_multiple_files=True)

if uploaded_files:
    with st.spinner("Processando arquivos..."):
        temp_dir = tempfile.mkdtemp()

        dados_2020_2021_2022 = pd.DataFrame()
        dados_geral = pd.DataFrame()

        for uploaded_file in uploaded_files:
            zip_path = os.path.join(temp_dir, uploaded_file.name)
            with open(zip_path, "wb") as f:
                f.write(uploaded_file.getbuffer())

            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(os.path.join(temp_dir, "extracted"))

        extracted_dir = os.path.join(temp_dir, "extracted")

        for dbf_file in sorted(os.listdir(extracted_dir)):
            if not dbf_file.lower().endswith(".dbf"):
                continue

            path = os.path.join(extracted_dir, dbf_file)
            dbf = DBF(path, encoding='latin-1', ignore_missing_memofile=True)
            dados = pd.DataFrame(iter(dbf))

            try:
                dados_filtrados = dados[
                    (dados['CLASSI_FIN'] == '5') &
                    (dados['ID_RG_RESI'] == '014 CRS') &
                    (dados['UTI'] == '1') &
                    (dados['DT_SAIDUTI'] == '')
                ]
                dados_filtrados = dados_filtrados.filter(['NM_PACIENT', 'ID_MN_RESI', 'DT_NOTIFIC'])
                dados_filtrados.columns = ['nome do paciente', 'municipio de residencia', 'data da notificacao']
                dados_2020_2021_2022 = pd.concat([dados_2020_2021_2022, dados_filtrados])
                dados_geral = pd.concat([dados_geral, dados])
            except KeyError:
                st.warning(f"Erro ao processar: {dbf_file} - verifique os campos esperados.")
        
        shutil.rmtree(temp_dir)

        dados_2020_2021_2022.reset_index(drop=True, inplace=True)
        dados_2020_2021_2022 = dados_2020_2021_2022.sort_values(by='municipio de residencia').drop_duplicates()

        st.subheader("Casos com UTI, CLASSI_FIN = 5, sem saída UTI")
        st.dataframe(dados_2020_2021_2022)

        st.subheader("Consolidado por município")
        dados_consolidados = dados_geral[
            (dados_geral['CLASSI_FIN'] == '5') &
            (dados_geral['ID_RG_RESI'] == '014 CRS')
        ]
        consolidado = dados_consolidados.groupby('ID_MN_RESI')['ID_RG_RESI'].count().reset_index()
        consolidado.columns = ['municipio de residencia', 'total']
        st.dataframe(consolidado)

        # Processamento detalhado
        dados_consolidados3 = dados_geral[dados_geral['ID_RG_RESI'] == '014 CRS']
        campos = ['NM_PACIENT', 'ID_MN_RESI', "DT_NOTIFIC", 'DT_SIN_PRI', 'CRITERIO', 'UTI', 'DT_SAIDUTI',
                  'CLASSI_FIN', 'EVOLUCAO', 'DT_EVOLUCAO', 'PCR_RESUL', 'TP_FLU_PCR', 'PCR_FLUASU',
                  'PCR_FLUBLI', 'PCR_VSR', 'PCR_PARA1', 'PCR_PARA2', 'PCR_PARA3', 'PCR_PARA4',
                  'PCR_ADENO', 'PCR_RINO']

        dados_consolidados2 = dados_consolidados3[campos].copy()

        # Substituições
        dados_consolidados2['criterio_texto'] = dados_consolidados2['CRITERIO'].replace({
            '1': 'laboratorial', '2': 'clinico-epidemiologico', '3': 'clinico', '4': 'clinico-imagem'})
        dados_consolidados2['UTI_texto'] = dados_consolidados2['UTI'].replace({'1': 'Sim', '2': 'Não', '9': 'Ignorado'})
        dados_consolidados2['Classificação final'] = dados_consolidados2['CLASSI_FIN'].replace({
            '1': 'SRAG por influenza', '2': 'SRAG por outro vírus respiratório', '3': 'SRAG por outro agente etiológico',
            '4': 'SRAG não especificado', '5': 'SRAG por Covid-19', '': 'Suspeito'})
        dados_consolidados2['Evolução'] = dados_consolidados2['EVOLUCAO'].replace({
            '1': 'Cura', '2': 'Óbito', '3': 'Óbito por outras causas', '9': 'Ignorado', '': 'Aguardando'})
        dados_consolidados2['PCR_RESUL'] = dados_consolidados2['PCR_RESUL'].replace({
            '1': 'Detectável', '2': 'Não detectável', '3': 'Inconclusivo',
            '4': 'Não realizado', '5': 'Aguardando resultado', '9': 'Ignorado'})
        dados_consolidados2['TP_FLU_PCR'] = dados_consolidados2['TP_FLU_PCR'].replace({'1': 'Influenza A', '2': 'Influenza B'})
        dados_consolidados2['PCR_FLUASU'] = dados_consolidados2['PCR_FLUASU'].replace({
            '1': 'Influenza A (H1N1)', '2': 'Influenza A (H3N2)', '3': 'Influenza não subtipado',
            '4': 'Influenza não subtipável', '5': 'Inconclusivo', '6': 'Outro'})
        dados_consolidados2['PCR_FLUBLI'] = dados_consolidados2['PCR_FLUBLI'].replace({
            '1': 'Victoria', '2': 'Yamagatha', '3': 'Não realizado', '4': 'Inconclusivo', '5': 'Outro'})

        lista_virusresp = ['PCR_VSR', 'PCR_PARA1', 'PCR_PARA2', 'PCR_PARA3', 'PCR_PARA4', 'PCR_ADENO', 'PCR_RINO']
        for virus in lista_virusresp:
            dados_consolidados2[virus] = dados_consolidados2[virus].replace({'1': 'Sim', ' ': ' '})

        dados_consolidados2 = dados_consolidados2[[
            'NM_PACIENT', 'ID_MN_RESI', 'DT_NOTIFIC', 'DT_SIN_PRI', 'UTI_texto',
            'DT_SAIDUTI', 'criterio_texto', 'Classificação final', 'Evolução', 'PCR_RESUL',
            'TP_FLU_PCR', 'PCR_FLUASU', 'PCR_FLUBLI', 'PCR_VSR', 'PCR_PARA1', 'PCR_PARA2',
            'PCR_PARA3', 'PCR_PARA4', 'PCR_ADENO', 'PCR_RINO'
        ]]
        dados_consolidados2.columns = ['nome', 'municipio de residencia', 'data de notificacao', 'inicio dos sintomas',
                                       'Foi para UTI?', 'Data de saída da UTI', 'Critério de confirmação',
                                       'Classificação final', 'Evolução', 'Resultado outro PCR', 'Tipo Influenza',
                                       'subtipo Influenza A', 'subtipo Influenza B', 'VSR', 'PARA1', 'PARA2', 'PARA3',
                                       'PARA4', 'ADENO', 'RINO']

        dados_consolidados2['data de notificacao'] = pd.to_datetime(dados_consolidados2['data de notificacao'], errors='coerce', dayfirst=True)
        dados_consolidados2 = dados_consolidados2.sort_values(by='data de notificacao')
        dados_consolidados2['data de notificacao'] = dados_consolidados2['data de notificacao'].astype(str)

        st.subheader("Base consolidada detalhada")
        st.dataframe(dados_consolidados2, use_container_width=True)

        csv = dados_consolidados2.to_csv(index=False).encode('utf-8')
        st.download_button("Baixar consolidado CSV", data=csv, file_name="consolidado_srag.csv", mime='text/csv')
