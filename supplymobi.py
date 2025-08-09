import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime
import warnings
import sqlite3
import os
from datetime import datetime

warnings.filterwarnings('ignore')


# Função para inicializar o banco de dados
def init_database():
    """Inicializa o banco de dados SQLite"""
    conn = sqlite3.connect('supply_chain.db', check_same_thread=False )
    cursor = conn.cursor()

    # Criar tabela para SC's
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS scs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data DATE,
            descricao TEXT,
            status TEXT,
            prioridade TEXT,
            solicitante TEXT,
            departamento TEXT,
            categoria TEXT,
            data_compra DATE,
            pedido INTEGER,
            tmc INTEGER,
            pmp INTEGER,
            valor REAL,
            fornecedor TEXT,
            comprador TEXT,
            upload_timestamp DATETIME
        )
    ''')

    # Criar tabela para Saving
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS saving (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data DATE,
            numero_pedido INTEGER,
            fornecedor TEXT,
            valor_inicial REAL,
            valor_final REAL,
            reducao_reais REAL,
            reducao_percentual REAL,
            comentarios_negociacao TEXT,
            tipo_saving TEXT,
            comprador TEXT,
            upload_timestamp DATETIME
        )
    ''')

    # Criar tabela para controle de uploads
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS upload_control (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            last_update DATETIME,
            filename TEXT,
            total_scs INTEGER,
            total_saving INTEGER
        )
    ''')

    conn.commit()
    conn.close()


# Função para salvar dados no banco
def save_to_database(scs_df, saving_df, filename):
    """Salva os dados no banco SQLite substituindo os anteriores"""
    conn = sqlite3.connect('supply_chain.db', check_same_thread=False )

    try:
        # Limpar dados anteriores
        conn.execute('DELETE FROM scs')
        conn.execute('DELETE FROM saving')
        conn.execute('DELETE FROM upload_control')

        # Timestamp do upload
        upload_time = datetime.now()

        # Preparar dados SCs para inserção
        scs_data = scs_df.copy()
        scs_data['upload_timestamp'] = upload_time

        # Mapear colunas do DataFrame para o banco
        scs_columns_map = {
            'Data': 'data',
            'Descrição': 'descricao',
            'Status': 'status',
            'Prioridade': 'prioridade',
            'Solicitante': 'solicitante',
            'Departamento': 'departamento',
            'Categoria': 'categoria',
            'Data da Compra': 'data_compra',
            'Pedido': 'pedido',
            'TMC': 'tmc',
            'PMP': 'pmp',
            'Valor': 'valor',
            'Fornecedor': 'fornecedor',
            'Comprador': 'comprador'
        }

        # Renomear colunas
        scs_renamed = scs_data.rename(columns=scs_columns_map)

        # Inserir dados SCs
        scs_renamed.to_sql('scs', conn, if_exists='append', index=False)

        # Preparar dados Saving para inserção
        saving_data = saving_df.copy()
        saving_data['upload_timestamp'] = upload_time

        # Mapear colunas Saving
        saving_columns_map = {
            'Data': 'data',
            'Número Pedido': 'numero_pedido',
            'Fornecedor': 'fornecedor',
            'VALOR INICIAL': 'valor_inicial',
            'VALOR FINAL': 'valor_final',
            'Redução R$': 'reducao_reais',
            'Redução %': 'reducao_percentual',
            'Comentários Negocição': 'comentarios_negociacao',
            'Tipo de Saving': 'tipo_saving',
            'Comprador': 'comprador'
        }

        # Renomear colunas
        saving_renamed = saving_data.rename(columns=saving_columns_map)

        # Inserir dados Saving
        saving_renamed.to_sql('saving', conn, if_exists='append', index=False)

        # Registrar controle do upload
        conn.execute('''
            INSERT INTO upload_control (last_update, filename, total_scs, total_saving)
            VALUES (?, ?, ?, ?)
        ''', (upload_time, filename, len(scs_df), len(saving_df)))

        conn.commit()
        return True, upload_time

    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()


# Função para carregar dados do banco
@st.cache_data
def load_from_database():
    """Carrega dados do banco SQLite"""
    if not os.path.exists('supply_chain.db'):
        return None, None, None

    conn = sqlite3.connect('supply_chain.db', check_same_thread=False)

    try:
        # Carregar SCs
        scs_df = pd.read_sql_query('SELECT * FROM scs', conn)

        # Carregar Saving
        saving_df = pd.read_sql_query('SELECT * FROM saving', conn)

        # Carregar info do último upload
        upload_info = pd.read_sql_query('''
            SELECT last_update, filename, total_scs, total_saving 
            FROM upload_control 
            ORDER BY last_update DESC 
            LIMIT 1
        ''', conn)

        if not scs_df.empty:
            # Converter colunas de data
            scs_df['data'] = pd.to_datetime(scs_df['data'])
            scs_df['data_compra'] = pd.to_datetime(scs_df['data_compra'])

        if not saving_df.empty:
            saving_df['data'] = pd.to_datetime(saving_df['data'])

        # Renomear colunas de volta para o padrão original
        scs_columns_reverse = {
            'data': 'Data',
            'descricao': 'Descrição',
            'status': 'Status',
            'prioridade': 'Prioridade',
            'solicitante': 'Solicitante',
            'departamento': 'Departamento',
            'categoria': 'Categoria',
            'data_compra': 'Data da Compra',
            'pedido': 'Pedido',
            'tmc': 'TMC',
            'pmp': 'PMP',
            'valor': 'Valor',
            'fornecedor': 'Fornecedor',
            'comprador': 'Comprador'
        }

        saving_columns_reverse = {
            'data': 'Data',
            'numero_pedido': 'Número Pedido',
            'fornecedor': 'Fornecedor',
            'valor_inicial': 'VALOR INICIAL',
            'valor_final': 'VALOR FINAL',
            'reducao_reais': 'Redução R$',
            'reducao_percentual': 'Redução %',
            'comentarios_negociacao': 'Comentários Negocição',
            'tipo_saving': 'Tipo de Saving',
            'comprador': 'Comprador'
        }

        scs_df = scs_df.rename(columns=scs_columns_reverse)
        saving_df = saving_df.rename(columns=saving_columns_reverse)

        return scs_df, saving_df, upload_info

    except Exception as e:
        st.error(f"Erro ao carregar do banco: {str(e)}")
        return None, None, None
    finally:
        conn.close()


# Função para exibir informações do último upload
def display_last_update_info(upload_info):
    """Exibe informações da última atualização"""
    if upload_info is not None and not upload_info.empty:
        last_update = pd.to_datetime(upload_info.iloc[0]['last_update'])
        filename = upload_info.iloc[0]['filename']
        total_scs = upload_info.iloc[0]['total_scs']
        total_saving = upload_info.iloc[0]['total_saving']

        # Formatar data brasileira
        formatted_date = last_update.strftime("%d/%m/%Y às %H:%M:%S")

        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #28a745 0%, #20c997 100%); 
                    color: white; padding: 1rem; border-radius: 8px; margin: 1rem 0;
                    box-shadow: 0 2px 10px rgba(40, 167, 69, 0.3);">
            <h4 style="margin: 0 0 0.5rem 0;">📊 Informações da Base de Dados</h4>
            <p style="margin: 0;"><strong>📅 Última Atualização:</strong> {formatted_date}</p>
            <p style="margin: 0;"><strong>📁 Arquivo:</strong> {filename}</p>
            <p style="margin: 0;"><strong>📈 Registros SC's:</strong> {total_scs} | <strong>💰 Registros Saving:</strong> {total_saving}</p>
        </div>
        """, unsafe_allow_html=True)

# Configuração da página
st.set_page_config(
    page_title="Dashboard Supply Chain",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS customizado com identidade visual Mobi Transportes
st.markdown("""
<style>
    /* Reset e configurações base */
    .main .block-container {
        padding-top: 1rem;
        padding-bottom: 1rem;
        max-width: 100%;
    }

    /* Header principal */
    .main-header {
        font-size: clamp(1.8rem, 4vw, 2.5rem);
        font-weight: 700;
        color: #EF8740;
        text-align: center;
        margin-bottom: 1.5rem;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
        background: linear-gradient(135deg, #EF8740 0%, #000000 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }

    /* Cards KPI otimizados */
    .metric-card {
        background: linear-gradient(135deg, #EF8740 0%, #000000 90%);
        padding: 1rem;
        border-radius: 12px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 20px rgba(239, 135, 64, 0.3);
        margin: 0.5rem 0;
        min-height: 120px;
        display: flex;
        flex-direction: column;
        justify-content: center;
    }

    .metric-value {
        font-size: clamp(1.2rem, 3vw, 1.8rem);
        font-weight: 700;
        margin: 0.2rem 0;
        line-height: 1.2;
    }

    .metric-label {
        font-size: clamp(0.7rem, 2vw, 0.9rem);
        opacity: 0.95;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        line-height: 1.1;
    }

    /* Headers de seção */
    .section-header {
        font-size: clamp(1.2rem, 3vw, 1.6rem);
        font-weight: 600;
        color: #000000;
        margin: 1.5rem 0 1rem 0;
        border-bottom: 3px solid #EF8740;
        padding-bottom: 0.5rem;
    }

    /* Alertas de auditoria */
    .audit-alert {
        background: linear-gradient(135deg, #dc3545 0%, #c82333 100%);
        color: white;
        padding: 0.8rem;
        border-radius: 8px;
        margin: 0.5rem 0;
        box-shadow: 0 2px 10px rgba(220, 53, 69, 0.3);
        font-size: 0.9rem;
    }

    .audit-success {
        background: linear-gradient(135deg, #28a745 0%, #20c997 100%);
        color: white;
        padding: 0.8rem;
        border-radius: 8px;
        margin: 0.5rem 0;
        box-shadow: 0 2px 10px rgba(40, 167, 69, 0.3);
        font-size: 0.9rem;
    }

    /* Sidebar responsiva */
    .sidebar .sidebar-content {
        background: linear-gradient(180deg, #f8f9fa 0%, #e9ecef 100%);
    }

    /* Otimizações para mobile */
    @media (max-width: 768px) {
        .main .block-container {
            padding-left: 1rem;
            padding-right: 1rem;
        }

        .metric-card {
            min-height: 100px;
            padding: 0.8rem;
        }

        .section-header {
            margin: 1rem 0 0.8rem 0;
        }

        /* Ocultar sidebar em mobile por padrão */
        .sidebar .sidebar-content {
            width: 100% !important;
        }
    }

    @media (max-width: 480px) {
        .main-header {
            margin-bottom: 1rem;
        }

        .metric-card {
            min-height: 90px;
            padding: 0.6rem;
        }

        .metric-value {
            font-size: 1.1rem;
        }

        .metric-label {
            font-size: 0.7rem;
        }
    }

    /* Estilo para tabelas responsivas */
    .dataframe {
        font-size: clamp(0.7rem, 2vw, 0.9rem);
    }

    /* Upload area */
    .uploadedFile {
        border: 2px dashed #EF8740;
        border-radius: 8px;
        padding: 1rem;
        text-align: center;
        background: rgba(239, 135, 64, 0.05);
    }
</style>
""", unsafe_allow_html=True)


# Função para criar KPI cards
def create_kpi_card(value, label, format_type="currency"):
    if format_type == "currency":
        formatted_value = f"R$ {value:,.2f}"
    elif format_type == "percentage":
        formatted_value = f"{value:.1f}%"
    elif format_type == "days":
        formatted_value = f"{value:.1f} dias"
    else:
        formatted_value = f"{value:,.0f}"

    return f"""
    <div class="metric-card">
        <div class="metric-value">{formatted_value}</div>
        <div class="metric-label">{label}</div>
    </div>
    """


# Função para carregar dados
@st.cache_data
def load_data(uploaded_file):
    if uploaded_file is not None:
        try:
            # Carregar o arquivo Excel enviado pelo usuário
            scs_df = pd.read_excel(uploaded_file, sheet_name="SC's")
            saving_df = pd.read_excel(uploaded_file, sheet_name="Saving")
            return scs_df, saving_df
        except Exception as e:
            st.error(f"Erro ao carregar o arquivo: {str(e)}")
            return None, None
    return None, None


# Função para criar dados de exemplo
def create_sample_data():
    # Dados de exemplo para SC's
    scs_data = {
        'Data': pd.to_datetime(['2025-07-14', '2025-07-15', '2025-07-16', '2025-07-17', '2025-07-18'] * 4),
        'Descrição': ['SENSOR BOIA TANQUE', 'VÁLVULA CONTROLE', 'FILTRO ÓLEO', 'BOMBA HIDRÁULICA', 'PARAFUSO INOX'] * 4,
        'Status': ['Concluido'] * 20,
        'Prioridade': ['Emergente', 'Normal', 'Urgente', 'Normal', 'Emergente'] * 4,
        'Solicitante': ['DANILO - ALMOX', 'CARLOS - PROD', 'MARIA - MAINT', 'JOÃO - ALMOX', 'ANA - PROD'] * 4,
        'Departamento': ['MANUTENÇÃO', 'PRODUÇÃO', 'MANUTENÇÃO', 'ALMOXARIFADO', 'PRODUÇÃO'] * 4,
        'Categoria': ['TANQUE', 'HIDRÁULICO', 'FILTROS', 'BOMBAS', 'FIXAÇÃO'] * 4,
        'Data da Compra': pd.to_datetime(['2025-07-14', '2025-07-15', '2025-07-16', '2025-07-17', '2025-07-18'] * 4),
        'Pedido': [122978, 123058, 123059, 123060, 123061] * 4,
        'TMC': [2, 5, 3, 7, 4] * 4,  # Tempo médio de compras em dias
        'PMP': [30, 45, 35, 60, 25] * 4,  # Prazo médio de pagamento (PMPS)
        'Valor': [235.00, 1286.00, 450.50, 2150.00, 180.75] * 4,
        'Fornecedor': ['BELCAR', 'BUENOS', 'HIDROSUL', 'BOMBASTECH', 'INOXPAR'] * 4,
        'Comprador': ['MATHEUS', 'CARLOS', 'ANA', 'MATHEUS', 'CARLOS'] * 4
    }

    # Dados de exemplo para Saving
    saving_data = {
        'Data': pd.to_datetime(['2025-07-15', '2025-07-16', '2025-07-17']),
        'Número Pedido': [123058, 123059, 123060],
        'Fornecedor': ['BUENOS', 'HIDROSUL', 'BOMBASTECH'],
        'VALOR INICIAL': [1286.00, 450.50, 2150.00],
        'VALOR FINAL': [1180.00, 420.00, 2000.00],
        'Redução R$': [106.00, 30.50, 150.00],
        'Redução %': [8.24, 6.77, 6.98],
        'Comentários': ['NEGOCIAÇÃO', 'DESCONTO VOLUME', 'NEGOCIAÇÃO PRAZO'],
        'Negocição': ['Negociação', 'Desconto', 'Negociação'],
        'Tipo de Saving': ['Negociação', 'Volume', 'Prazo'],
        'Comprador': ['CARLOS', 'ANA', 'MATHEUS']
    }

    return pd.DataFrame(scs_data), pd.DataFrame(saving_data)


# Função principal
def main():
    # Inicializar banco de dados
    init_database()

    st.markdown('<h1 class="main-header">🚛 Dashboard Supply Chain - Mobi Transportes</h1>', unsafe_allow_html=True)

    # Upload do arquivo
    st.markdown("### 📁 Upload do Arquivo de Dados")
    uploaded_file = st.file_uploader(
        "Faça upload do arquivo 'KPIs- Compras (Base de Dados).xlsx'",
        type=['xlsx', 'xls'],
        help="Arquivo deve conter as abas 'SC's' e 'Saving'"
    )

    # Carregar dados
    scs_df = None
    saving_df = None
    upload_info = None

    if uploaded_file is not None:
        # Processar novo upload
        scs_df, saving_df = load_data(uploaded_file)
        # Adicione este código logo após o título principal, antes dos filtros da sidebar

        if upload_info is not None and not upload_info.empty:
            last_update = pd.to_datetime(upload_info.iloc[0]['last_update'])
            formatted_date = last_update.strftime("%d/%m/%Y às %H:%M")

            st.markdown(f"""
           <div style="background: linear-gradient(135deg, #EF8740 0%, #000000 90%); 
                       color: white; padding: 1rem; border-radius: 8px; margin: 1rem 0;
                       text-align: center; box-shadow: 0 2px 10px rgba(239, 135, 64, 0.3);">
               <h4 style="margin: 0; font-weight: 600;">📅 Última Atualização: {formatted_date}</h4>
           </div>
           """, unsafe_allow_html=True)

        if scs_df is not None and saving_df is not None:
            # Salvar no banco de dados
            success, result = save_to_database(scs_df, saving_df, uploaded_file.name)

            if success:
                st.success("✅ Arquivo carregado e salvo no banco de dados com sucesso!")
                st.markdown("---")

                # Limpar cache para forçar reload dos dados
                load_from_database.clear()

                # Carregar dados atualizados do banco
                scs_df, saving_df, upload_info = load_from_database()

                # Mostrar informações da atualização
                display_last_update_info(upload_info)

            else:
                st.error(f"❌ Erro ao salvar no banco de dados: {result}")
                st.markdown("### 🔍 Usando dados do upload atual")
        else:
            st.error("❌ Erro ao carregar o arquivo. Verifique se ele contém as abas 'SC's' e 'Saving'.")
    else:
        # Tentar carregar dados existentes do banco
        scs_df, saving_df, upload_info = load_from_database()

        if scs_df is not None and saving_df is not None:
            st.info("📤 Dados carregados do banco de dados local. Faça upload de um novo arquivo para atualizar.")
            # Mostrar informações da última atualização
            display_last_update_info(upload_info)
            st.markdown("---")
        else:
            st.info("📤 Por favor, faça upload do arquivo Excel para começar a análise.")
            st.markdown("---")
            st.markdown("### 🔍 Preview com Dados de Exemplo")
            st.info("Enquanto isso, você pode ver como o dashboard funciona com dados de exemplo:")

            # Usar dados de exemplo
            scs_df, saving_df = create_sample_data()
            st.warning("⚠️ Os dados mostrados abaixo são apenas exemplos para demonstração.")

    # Sidebar com filtros
    if scs_df is not None and not scs_df.empty:
        st.sidebar.markdown("## 🔧 Filtros")

        # Filtro por comprador
        compradores = ['Todos'] + list(scs_df['Comprador'].unique())
        comprador_selecionado = st.sidebar.selectbox("Comprador:", compradores)

        # Filtro por período
        data_inicio = st.sidebar.date_input("Data Início:", min(scs_df['Data']))
        data_fim = st.sidebar.date_input("Data Fim:", max(scs_df['Data']))

        # Aplicar filtros
        scs_filtered = scs_df.copy()
        if comprador_selecionado != 'Todos':
            scs_filtered = scs_filtered[scs_filtered['Comprador'] == comprador_selecionado]

        scs_filtered = scs_filtered[
            (scs_filtered['Data'] >= pd.to_datetime(data_inicio)) &
            (scs_filtered['Data'] <= pd.to_datetime(data_fim))
            ]
    else:
        return  # Sair da função se não há dados para processar

    # === SEÇÃO 1: SPENDING ANALYSIS ===
    st.markdown('<div class="section-header">💰 Análise de Gastos</div>', unsafe_allow_html=True)

    # Layout responsivo
    col1, col2 = st.columns([3, 1])  # Proporção ajustada

    with col1:
        # Gráfico Spend por comprador
        spend_por_comprador = scs_filtered.groupby('Comprador')['Valor'].sum().reset_index()
        fig_spend = px.bar(
            spend_por_comprador,
            x='Comprador',
            y='Valor',
            title="💳 Spend Total por Comprador",
            text='Valor'
        )
        # Aplicar cores da Mobi Transportes
        fig_spend.update_traces(
            marker_color='#EF8740',
            texttemplate='<b>R$ %{text:,.0f}</b>',
            textposition='outside',
            textfont_size=14,
            textfont_color='#000000',
            width=0.6  # Barras mais estreitas
        )
        fig_spend.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(size=11, color='#000000'),
            title_font_size=14,
            title_font_color='#000000',
            margin=dict(l=20, r=20, t=50, b=50),  # Margem superior aumentada
            height=450,  # Altura aumentada para acomodar rótulos
            yaxis=dict(range=[0, spend_por_comprador['Valor'].max() * 1.15])  # 15% acima do valor máximo
        )
        st.plotly_chart(fig_spend, use_container_width=True)

    with col2:
        # KPI Spend Total
        spend_total = scs_filtered['Valor'].sum()
        st.markdown(create_kpi_card(spend_total, "Spend Total"), unsafe_allow_html=True)

    # === SEÇÃO 2: TEMPO MÉDIO DE COMPRAS ===
    st.markdown('''
    <div class="section-header">
        ⏱️ Análise de Tempo (TMC) 
        <span class="tooltip-container">
            <span class="help-icon">?</span>
            <div class="tooltip-content">
                <div class="tooltip-arrow"></div>
                <strong>TMC - Tempo Médio de Compras</strong><br><br>
                Métrica que indica quantos dias em média cada comprador leva para processar e finalizar uma compra, desde a solicitação até a conclusão do pedido.<br><br>
                <span style="color: #4CAF50;">✓ Valores menores = Maior eficiência operacional</span>
            </div>
        </span>
    </div>

    <style>
        .tooltip-container {
            position: relative;
            display: inline-block;
            margin-left: 8px;
        }

        .help-icon {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 18px;
            height: 18px;
            background: linear-gradient(135deg, #EF8740, #FF6B35);
            color: white;
            border-radius: 50%;
            font-size: 12px;
            font-weight: bold;
            cursor: help;
            transition: all 0.3s ease;
            box-shadow: 0 2px 8px rgba(239, 135, 64, 0.3);
        }

        .help-icon:hover {
            transform: scale(1.1);
            box-shadow: 0 4px 15px rgba(239, 135, 64, 0.5);
        }

        .tooltip-content {
            position: absolute;
            bottom: 130%;
            left: 50%;
            transform: translateX(-50%);
            background: linear-gradient(145deg, #2c3e50, #34495e);
            color: white;
            padding: 16px 20px;
            border-radius: 12px;
            white-space: nowrap;
            opacity: 0;
            visibility: hidden;
            transition: all 0.3s cubic-bezier(0.68, -0.55, 0.265, 1.55);
            z-index: 1000;
            min-width: 320px;
            max-width: 400px;
            white-space: normal;
            text-align: left;
            font-size: 13px;
            line-height: 1.4;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
            border: 1px solid rgba(255, 255, 255, 0.1);
        }

        .tooltip-arrow {
            position: absolute;
            top: 100%;
            left: 50%;
            transform: translateX(-50%);
            width: 0;
            height: 0;
            border-left: 8px solid transparent;
            border-right: 8px solid transparent;
            border-top: 8px solid #2c3e50;
        }

        .tooltip-container:hover .tooltip-content {
            opacity: 1;
            visibility: visible;
            transform: translateX(-50%) translateY(-5px);
        }

        @media (max-width: 768px) {
            .tooltip-content {
                min-width: 280px;
                font-size: 12px;
                padding: 14px 16px;
            }
        }
    </style>
    ''', unsafe_allow_html=True)

    col1, col2 = st.columns([3, 1])

    with col1:
        # Gráfico TMC por comprador
        tmc_por_comprador = scs_filtered.groupby('Comprador')['TMC'].mean().reset_index()
        fig_tmc = px.bar(
            tmc_por_comprador,
            x='Comprador',
            y='TMC',
            title="📅 Tempo Médio de Compras por Comprador",
            text='TMC'
        )
        fig_tmc.update_traces(
            marker_color='#EF8740',
            texttemplate='<b>%{text:.1f} dias</b>',
            textposition='outside',
            textfont_size=14,
            textfont_color='#000000',
            width=0.6
        )
        fig_tmc.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(size=11, color='#000000'),
            title_font_size=14,
            title_font_color='#000000',
            margin=dict(l=20, r=20, t=50, b=50),
            height=450,
            yaxis=dict(range=[0, tmc_por_comprador['TMC'].max() * 1.15])
        )
        st.plotly_chart(fig_tmc, use_container_width=True)

    with col2:
        # KPI TMC Geral
        tmc_geral = scs_filtered['TMC'].mean()
        st.markdown(create_kpi_card(tmc_geral, "TMC Médio Geral", "days"), unsafe_allow_html=True)

    # === SEÇÃO 3: PMPS (Prazo Médio de Pagamento Simples) ===
    st.markdown('''
    <div class="section-header">
        💳 Análise PMPS - Prazo Médio de Pagamento Simples
        <span class="tooltip-container">
            <span class="help-icon">?</span>
            <div class="tooltip-content">
                <div class="tooltip-arrow"></div>
                <strong>PMPS - Prazo Médio de Pagamento Simples</strong><br><br>
                Métrica que calcula a média aritmética simples dos prazos de pagamento de todas as compras, independente do valor de cada uma. Todas as compras têm o mesmo peso no cálculo.<br><br>
                <span style="color: #4CAF50;">📊 Fórmula: Σ(Prazos) ÷ Quantidade de Compras</span><br>
                <span style="color: #2196F3;">📈 Ideal para análise de frequência operacional</span>
            </div>
        </span>
    </div>

    <style>
        .tooltip-container {
            position: relative;
            display: inline-block;
            margin-left: 8px;
        }

        .help-icon {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 18px;
            height: 18px;
            background: linear-gradient(135deg, #EF8740, #FF6B35);
            color: white;
            border-radius: 50%;
            font-size: 12px;
            font-weight: bold;
            cursor: help;
            transition: all 0.3s ease;
            box-shadow: 0 2px 8px rgba(239, 135, 64, 0.3);
        }

        .help-icon:hover {
            transform: scale(1.1);
            box-shadow: 0 4px 15px rgba(239, 135, 64, 0.5);
        }

        .tooltip-content {
            position: absolute;
            bottom: 130%;
            left: 50%;
            transform: translateX(-50%);
            background: linear-gradient(145deg, #2c3e50, #34495e);
            color: white;
            padding: 16px 20px;
            border-radius: 12px;
            white-space: nowrap;
            opacity: 0;
            visibility: hidden;
            transition: all 0.3s cubic-bezier(0.68, -0.55, 0.265, 1.55);
            z-index: 1000;
            min-width: 380px;
            max-width: 450px;
            white-space: normal;
            text-align: left;
            font-size: 13px;
            line-height: 1.4;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
            border: 1px solid rgba(255, 255, 255, 0.1);
        }

        .tooltip-arrow {
            position: absolute;
            top: 100%;
            left: 50%;
            transform: translateX(-50%);
            width: 0;
            height: 0;
            border-left: 8px solid transparent;
            border-right: 8px solid transparent;
            border-top: 8px solid #2c3e50;
        }

        .tooltip-container:hover .tooltip-content {
            opacity: 1;
            visibility: visible;
            transform: translateX(-50%) translateY(-5px);
        }

        @media (max-width: 768px) {
            .tooltip-content {
                min-width: 320px;
                font-size: 12px;
                padding: 14px 16px;
            }
        }
    </style>
    ''', unsafe_allow_html=True)

    col1, col2 = st.columns([3, 1])

    with col1:
        # Gráfico PMPS por comprador
        pmps_por_comprador = scs_filtered.groupby('Comprador')['PMP'].mean().reset_index()
        fig_pmps = px.bar(
            pmps_por_comprador,
            x='Comprador',
            y='PMP',
            title="📋 PMPS por Comprador",
            text='PMP'
        )
        fig_pmps.update_traces(
            marker_color='#EF8740',
            texttemplate='<b>%{text:.1f} dias</b>',
            textposition='outside',
            textfont_size=14,
            textfont_color='#000000',
            width=0.6
        )
        fig_pmps.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(size=11, color='#000000'),
            title_font_size=14,
            title_font_color='#000000',
            margin=dict(l=20, r=20, t=50, b=50),
            height=450,
            yaxis=dict(range=[0, pmps_por_comprador['PMP'].max() * 1.15])
        )
        st.plotly_chart(fig_pmps, use_container_width=True)

    with col2:
        # KPI PMPS Geral
        pmps_geral = scs_filtered['PMP'].mean()
        st.markdown(create_kpi_card(pmps_geral, "PMPS Médio Geral", "days"), unsafe_allow_html=True)

    # === SEÇÃO 4: PMPP (Prazo Médio de Pagamento Ponderado) ===
    st.markdown('''
    <div class="section-header">
        ⚖️ Análise PMPP - Prazo Médio de Pagamento Ponderado (R$)
        <span class="tooltip-container">
            <span class="help-icon">?</span>
            <div class="tooltip-content">
                <div class="tooltip-arrow"></div>
                <strong>PMPP - Prazo Médio de Pagamento Ponderado</strong><br><br>
                Métrica que calcula o prazo médio de pagamento considerando o peso (valor) de cada compra. Diferente do PMPS, que é uma média simples, o PMPP dá mais importância às compras de maior valor.<br><br>
                <span style="color: #4CAF50;">📊 Fórmula: Σ(Prazo × Valor) ÷ Σ(Valor)</span><br>
                <span style="color: #FFA726;">⚖️ Mais preciso para análise financeira</span>
            </div>
        </span>
    </div>

    <style>
        .tooltip-container {
            position: relative;
            display: inline-block;
            margin-left: 8px;
        }

        .help-icon {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 18px;
            height: 18px;
            background: linear-gradient(135deg, #EF8740, #FF6B35);
            color: white;
            border-radius: 50%;
            font-size: 12px;
            font-weight: bold;
            cursor: help;
            transition: all 0.3s ease;
            box-shadow: 0 2px 8px rgba(239, 135, 64, 0.3);
        }

        .help-icon:hover {
            transform: scale(1.1);
            box-shadow: 0 4px 15px rgba(239, 135, 64, 0.5);
        }

        .tooltip-content {
            position: absolute;
            bottom: 130%;
            left: 50%;
            transform: translateX(-50%);
            background: linear-gradient(145deg, #2c3e50, #34495e);
            color: white;
            padding: 16px 20px;
            border-radius: 12px;
            white-space: nowrap;
            opacity: 0;
            visibility: hidden;
            transition: all 0.3s cubic-bezier(0.68, -0.55, 0.265, 1.55);
            z-index: 1000;
            min-width: 380px;
            max-width: 450px;
            white-space: normal;
            text-align: left;
            font-size: 13px;
            line-height: 1.4;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
            border: 1px solid rgba(255, 255, 255, 0.1);
        }

        .tooltip-arrow {
            position: absolute;
            top: 100%;
            left: 50%;
            transform: translateX(-50%);
            width: 0;
            height: 0;
            border-left: 8px solid transparent;
            border-right: 8px solid transparent;
            border-top: 8px solid #2c3e50;
        }

        .tooltip-container:hover .tooltip-content {
            opacity: 1;
            visibility: visible;
            transform: translateX(-50%) translateY(-5px);
        }

        @media (max-width: 768px) {
            .tooltip-content {
                min-width: 320px;
                font-size: 12px;
                padding: 14px 16px;
            }
        }
    </style>
    ''', unsafe_allow_html=True)

    col1, col2 = st.columns([3, 1])

    with col1:
        # Calcular PMPP por comprador
        pmpp_por_comprador = scs_filtered.groupby('Comprador').apply(
            lambda x: (x['PMP'] * x['Valor']).sum() / x['Valor'].sum()
        ).reset_index()
        pmpp_por_comprador.columns = ['Comprador', 'PMPP']

        fig_pmpp = px.bar(
            pmpp_por_comprador,
            x='Comprador',
            y='PMPP',
            title="⚖️ PMPP por Comprador",
            text='PMPP'
        )
        fig_pmpp.update_traces(
            marker_color='#EF8740',
            texttemplate='<b>%{text:.1f} dias</b>',
            textposition='outside',
            textfont_size=14,
            textfont_color='#000000',
            width=0.6
        )
        fig_pmpp.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(size=11, color='#000000'),
            title_font_size=14,
            title_font_color='#000000',
            margin=dict(l=20, r=20, t=50, b=50),
            height=450,
            yaxis=dict(range=[0, pmpp_por_comprador['PMPP'].max() * 1.15])
        )
        st.plotly_chart(fig_pmpp, use_container_width=True)

    with col2:
        # KPI PMPP Geral
        pmpp_geral = (scs_filtered['PMP'] * scs_filtered['Valor']).sum() / scs_filtered['Valor'].sum()
        st.markdown(create_kpi_card(pmpp_geral, "PMPP Médio Geral", "days"), unsafe_allow_html=True)

    # === SEÇÃO 5: ANÁLISE DE FORNECEDORES ===
    st.markdown('<div class="section-header">🏢 Top 5 Gastos por Fornecedor</div>', unsafe_allow_html=True)

    # Verificar se a coluna Fornecedor existe
    if 'Fornecedor' in scs_filtered.columns:
        # Calcular gastos por fornecedor e pegar top 5
        gastos_fornecedor = scs_filtered.groupby('Fornecedor')['Valor'].sum().reset_index()
        gastos_fornecedor = gastos_fornecedor.sort_values('Valor', ascending=False).head(5)

        fig_fornecedor = px.bar(
            gastos_fornecedor,
            x='Fornecedor',
            y='Valor',
            title="🏆 Top 5 Fornecedores por Gasto Total",
            text='Valor'
        )
        fig_fornecedor.update_traces(
            marker_color='#EF8740',
            texttemplate='<b>R$ %{text:,.0f}</b>',
            textposition='outside',
            textfont_size=14,
            textfont_color='#000000',
            width=0.6
        )
        fig_fornecedor.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(size=11, color='#000000'),
            title_font_size=14,
            title_font_color='#000000',
            margin=dict(l=20, r=20, t=50, b=50),
            height=450,
            xaxis_tickangle=-45,
            yaxis=dict(range=[0, gastos_fornecedor['Valor'].max() * 1.15])
        )
        st.plotly_chart(fig_fornecedor, use_container_width=True)
    else:
        st.warning("⚠️ Coluna 'Fornecedor' não encontrada nos dados")

    # === SEÇÃO: TOP 5 CATEGORIAS ===
    st.markdown('<div class="section-header">📦 Top 5 Gastos por Categoria</div>', unsafe_allow_html=True)

    # Verificar se existe a coluna Categoria (pode ser 'Categoria', coluna G, ou posição 6)
    categoria_col = None

    # Tentar encontrar a coluna por nome primeiro
    if 'Categoria' in scs_filtered.columns:
        categoria_col = 'Categoria'
    else:
        # Tentar pela posição (coluna G = posição 6)
        if len(scs_filtered.columns) > 6:
            categoria_col = scs_filtered.columns[6]

    if categoria_col is not None:
        # Calcular gastos por categoria e pegar top 5
        gastos_categoria = scs_filtered.groupby(categoria_col)['Valor'].sum().reset_index()
        gastos_categoria = gastos_categoria.sort_values('Valor', ascending=False).head(5)

        fig_categoria = px.bar(
            gastos_categoria,
            x=categoria_col,
            y='Valor',
            title="🏆 Top 5 Categorias por Gasto Total",
            text='Valor'
        )
        fig_categoria.update_traces(
            marker_color='#EF8740',
            texttemplate='<b>R$ %{text:,.0f}</b>',
            textposition='outside',
            textfont_size=14,
            textfont_color='#000000',
            width=0.6
        )
        fig_categoria.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(size=11, color='#000000'),
            title_font_size=14,
            title_font_color='#000000',
            margin=dict(l=20, r=20, t=50, b=50),
            height=450,
            xaxis_tickangle=-45,
            yaxis=dict(range=[0, gastos_categoria['Valor'].max() * 1.15])
        )
        st.plotly_chart(fig_categoria, use_container_width=True)
    else:
        st.warning("⚠️ Coluna 'Categoria' não encontrada nos dados (esperada na coluna G - posição 6)")

    # === SEÇÃO 6: ANÁLISE DE PRIORIDADES ===
    st.markdown('<div class="section-header">🎯 Análise de Prioridades</div>', unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        # Gráfico de pizza - Prioridades por Quantidade
        prioridade_counts = scs_filtered['Prioridade'].value_counts()
        fig_pizza_qtd = px.pie(
            values=prioridade_counts.values,
            names=prioridade_counts.index,
            title="📊 Distribuição por Quantidade",
            color_discrete_sequence=['#EF8740', '#000000', '#FFA366', '#333333', '#FFB580']
        )
        fig_pizza_qtd.update_traces(
            textposition='inside',
            textinfo='percent+label',
            textfont_size=13,
            textfont_color='white'
        )
        fig_pizza_qtd.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(size=11, color='#000000'),
            title_font_size=14,
            title_font_color='#000000',
            margin=dict(l=20, r=20, t=50, b=20),
            height=400
        )
        st.plotly_chart(fig_pizza_qtd, use_container_width=True)

    with col2:
        # Gráfico de barras - Prioridades por Valor (ordenado do maior para o menor)
        prioridade_valores = scs_filtered.groupby('Prioridade')['Valor'].sum().reset_index()
        prioridade_valores = prioridade_valores.sort_values('Valor', ascending=False)  # Ordenar do maior para o menor

        fig_bar_valor = px.bar(
            prioridade_valores,
            x='Prioridade',
            y='Valor',
            title="💰 Distribuição por Valor",
            text='Valor'
        )
        fig_bar_valor.update_traces(
            marker_color='#EF8740',
            texttemplate='<b>R$ %{text:,.0f}</b>',
            textposition='outside',
            textfont_size=14,
            textfont_color='#000000',
            width=0.6
        )
        fig_bar_valor.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(size=11, color='#000000'),
            title_font_size=14,
            title_font_color='#000000',
            margin=dict(l=20, r=20, t=50, b=50),
            height=400,
            yaxis=dict(range=[0, prioridade_valores['Valor'].max() * 1.15])
        )
        st.plotly_chart(fig_bar_valor, use_container_width=True)

    # Funções auxiliares para mapeamento de colunas
    def find_column(df, possible_names):
        """Encontra a primeira coluna que existe no DataFrame"""
        for name in possible_names:
            if name in df.columns:
                return name
        return None

    def get_column_by_position(df, position):
        """Retorna o nome da coluna pela posição (0-indexed)"""
        if position < len(df.columns):
            return df.columns[position]
        return None

    # === SEÇÃO 6: ANÁLISE DE SAVINGS ===
    st.markdown('''
    <div class="section-header">
        💎 Análise de Savings
        <span class="tooltip-container">
            <span class="help-icon">?</span>
            <div class="tooltip-content">
                <div class="tooltip-arrow"></div>
                <strong>Savings - Economia em Compras</strong><br><br>
                Representa o valor economizado através de negociações, descontos por volume, mudanças de fornecedor ou outras estratégias de redução de custos.<br><br>
                <span style="color: #4CAF50;">💰 Saving Total: Soma de todas as economias obtidas</span><br>
                <span style="color: #2196F3;">📊 % Saving: (Economia Total ÷ Valor Total das Compras) × 100</span><br>
                <span style="color: #FF9800;">🎯 Objetivo: Maximizar economias sem comprometer qualidade</span>
            </div>
        </span>
    </div>

    <style>
        .tooltip-container {
            position: relative;
            display: inline-block;
            margin-left: 8px;
        }

        .help-icon {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 18px;
            height: 18px;
            background: linear-gradient(135deg, #EF8740, #FF6B35);
            color: white;
            border-radius: 50%;
            font-size: 12px;
            font-weight: bold;
            cursor: help;
            transition: all 0.3s ease;
            box-shadow: 0 2px 8px rgba(239, 135, 64, 0.3);
        }

        .help-icon:hover {
            transform: scale(1.1);
            box-shadow: 0 4px 15px rgba(239, 135, 64, 0.5);
        }

        .tooltip-content {
            position: absolute;
            bottom: 130%;
            left: 50%;
            transform: translateX(-50%);
            background: linear-gradient(145deg, #2c3e50, #34495e);
            color: white;
            padding: 16px 20px;
            border-radius: 12px;
            white-space: nowrap;
            opacity: 0;
            visibility: hidden;
            transition: all 0.3s cubic-bezier(0.68, -0.55, 0.265, 1.55);
            z-index: 1000;
            min-width: 400px;
            max-width: 480px;
            white-space: normal;
            text-align: left;
            font-size: 13px;
            line-height: 1.4;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
            border: 1px solid rgba(255, 255, 255, 0.1);
        }

        .tooltip-arrow {
            position: absolute;
            top: 100%;
            left: 50%;
            transform: translateX(-50%);
            width: 0;
            height: 0;
            border-left: 8px solid transparent;
            border-right: 8px solid transparent;
            border-top: 8px solid #2c3e50;
        }

        .tooltip-container:hover .tooltip-content {
            opacity: 1;
            visibility: visible;
            transform: translateX(-50%) translateY(-5px);
        }

        @media (max-width: 768px) {
            .tooltip-content {
                min-width: 340px;
                font-size: 12px;
                padding: 14px 16px;
            }
        }
    </style>
    ''', unsafe_allow_html=True)

    if not saving_df.empty:
        # Mapear coluna de saving
        saving_col = find_column(saving_df, ['Redução R$', 'Reducao R$', 'Saving', 'Economia', 'Redução'])
        comprador_col_saving = find_column(saving_df, ['Comprador', 'Buyer', 'Responsável'])

        if saving_col and comprador_col_saving:
            col1, col2 = st.columns([3, 1])

            with col1:
                # Gráfico Saving por comprador
                saving_por_comprador = saving_df.groupby(comprador_col_saving)[saving_col].sum().reset_index()
                fig_saving = px.bar(
                    saving_por_comprador,
                    x=comprador_col_saving,
                    y=saving_col,
                    title="💰 Savings por Comprador",
                    text=saving_col
                )
                fig_saving.update_traces(
                    marker_color='#EF8740',
                    texttemplate='<b>R$ %{text:,.0f}</b>',
                    textposition='outside',
                    textfont_size=14,
                    textfont_color='#000000',
                    width=0.6
                )
                fig_saving.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font=dict(size=11, color='#000000'),
                    title_font_size=14,
                    title_font_color='#000000',
                    margin=dict(l=20, r=20, t=50, b=20),
                    height=400
                )
                st.plotly_chart(fig_saving, use_container_width=True)

            with col2:
                # KPI Saving Total
                saving_total = saving_df[saving_col].sum()
                st.markdown(create_kpi_card(saving_total, "Saving Total"), unsafe_allow_html=True)

            # Gráfico Percentual de Saving por Comprador (abaixo dos anteriores)
            if comprador_col_saving:
                st.markdown("#### 📈 Percentual de Saving por Comprador")

                # Calcular saving total por comprador (da aba Saving)
                saving_por_comprador_total = saving_df.groupby(comprador_col_saving)[saving_col].sum().reset_index()

                # Calcular compras totais por comprador (da aba SC's)
                compras_por_comprador = scs_filtered.groupby('Comprador')['Valor'].sum().reset_index()

                # Fazer merge dos dados
                percentual_saving = pd.merge(
                    saving_por_comprador_total,
                    compras_por_comprador,
                    left_on=comprador_col_saving,
                    right_on='Comprador',
                    how='inner'
                )

                # Calcular percentual de saving
                percentual_saving['Percentual_Saving'] = (percentual_saving[saving_col] / percentual_saving[
                    'Valor']) * 100

                # Ordenar do maior para o menor percentual
                percentual_saving = percentual_saving.sort_values('Percentual_Saving', ascending=False)

                fig_perc_saving = px.bar(
                    percentual_saving,
                    x='Comprador',
                    y='Percentual_Saving',
                    title="📊 % Saving por Comprador (Saving Total ÷ Compras Totais)",
                    text='Percentual_Saving'
                )
                fig_perc_saving.update_traces(
                    marker_color='#EF8740',
                    texttemplate='<b>%{text:.1f}%</b>',
                    textposition='outside',
                    textfont_size=14,
                    textfont_color='#000000',
                    width=0.6
                )
                fig_perc_saving.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font=dict(size=11, color='#000000'),
                    title_font_size=14,
                    title_font_color='#000000',
                    margin=dict(l=20, r=20, t=50, b=50),
                    height=450,
                    yaxis=dict(range=[0, percentual_saving['Percentual_Saving'].max() * 1.15])
                )
                st.plotly_chart(fig_perc_saving, use_container_width=True)
        else:
            st.warning("⚠️ Colunas de saving não encontradas na aba Saving")
    else:
        st.info("ℹ️ Nenhum dado de saving disponível")

    # === SEÇÃO 8: AUDITORIA ===
    st.markdown('<div class="section-header">🔍 Auditoria de Valores</div>', unsafe_allow_html=True)

    # Estrutura de dados removida (não exibir no dashboard)

    # Buscar colunas na aba Saving (coluna B = posição 2)
    pedido_col_saving = get_column_by_position(saving_df, 2)  # Coluna B
    valor_final_col = find_column(saving_df, ['VALOR FINAL', 'Valor Final', 'Valor_Final', 'ValorFinal'])

    # Buscar colunas na aba SC's (coluna I = posição 9)
    pedido_col_scs = get_column_by_position(scs_df, 9)  # Coluna I
    valor_col_scs = find_column(scs_df, ['Valor', 'VALOR', 'Valor Total', 'Total'])

    st.markdown("#### 🔗 Mapeamento de Colunas")
    col1, col2 = st.columns(2)

    with col1:
        st.info(f"**SC's - Pedido:** {pedido_col_scs}")
        st.info(f"**SC's - Valor:** {valor_col_scs}")

    with col2:
        st.info(f"**Saving - Pedido:** {pedido_col_saving}")
        st.info(f"**Saving - Valor Final:** {valor_final_col}")

    # Realizar auditoria apenas se todas as colunas foram encontradas
    if all([pedido_col_saving, valor_final_col, pedido_col_scs, valor_col_scs]):
        audit_results = []

        for _, saving_row in saving_df.iterrows():
            pedido_num = saving_row[pedido_col_saving]
            valor_final_saving = saving_row[valor_final_col]

            # Buscar pedido correspondente na aba SC's
            scs_match = scs_df[scs_df[pedido_col_scs] == pedido_num]

            if not scs_match.empty:
                valor_scs = scs_match[valor_col_scs].iloc[0]

                # Verificar se os valores são diferentes (tolerância de R$ 0.01)
                if abs(valor_scs - valor_final_saving) > 0.01:
                    audit_results.append({
                        'Pedido': pedido_num,
                        'Valor SC\'s': valor_scs,
                        'Valor Final Saving': valor_final_saving,
                        'Diferença': valor_scs - valor_final_saving,
                        'Status': 'DIVERGÊNCIA'
                    })
                else:
                    audit_results.append({
                        'Pedido': pedido_num,
                        'Valor SC\'s': valor_scs,
                        'Valor Final Saving': valor_final_saving,
                        'Diferença': 0.0,
                        'Status': 'OK'
                    })
    else:
        missing_cols = []
        if not pedido_col_saving:
            missing_cols.append("Pedido na aba Saving")
        if not valor_final_col:
            missing_cols.append("Valor Final na aba Saving")
        if not pedido_col_scs:
            missing_cols.append("Pedido na aba SC's")
        if not valor_col_scs:
            missing_cols.append("Valor na aba SC's")

        st.error(f"❌ **Não foi possível realizar a auditoria.**")
        st.error(f"**Colunas não encontradas:** {', '.join(missing_cols)}")
        audit_results = []

    if audit_results:
        audit_df = pd.DataFrame(audit_results)

        # Separar divergências
        divergencias = audit_df[audit_df['Status'] == 'DIVERGÊNCIA']
        conformes = audit_df[audit_df['Status'] == 'OK']

        col1, col2 = st.columns(2)

        with col1:
            st.markdown(f"### ✅ Pedidos Conformes: {len(conformes)}")
            if not conformes.empty:
                st.markdown('<div class="audit-success">Todos os valores estão corretos!</div>', unsafe_allow_html=True)
                st.dataframe(conformes[['Pedido', 'Valor SC\'s', 'Valor Final Saving']], use_container_width=True)

        with col2:
            st.markdown(f"### ⚠️ Divergências Encontradas: {len(divergencias)}")
            if not divergencias.empty:
                st.markdown('<div class="audit-alert">Atenção! Valores divergentes detectados:</div>',
                            unsafe_allow_html=True)
                st.dataframe(divergencias, use_container_width=True)
            else:
                st.markdown('<div class="audit-success">Nenhuma divergência encontrada!</div>', unsafe_allow_html=True)

    # === AUDITORIA DE DATAS ===
    st.markdown("#### 📅 Auditoria de Datas")

    # Buscar colunas de data
    data_col_scs = find_column(scs_df, ['Data', 'Data da Compra', 'Data Compra', 'DATA'])
    data_col_saving = find_column(saving_df, ['Data', 'DATA', 'Data Saving'])

    if all([pedido_col_saving, data_col_saving, pedido_col_scs, data_col_scs]):
        audit_dates_results = []

        for _, saving_row in saving_df.iterrows():
            pedido_num = saving_row[pedido_col_saving]
            data_saving = pd.to_datetime(saving_row[data_col_saving]).date()

            # Buscar pedido correspondente na aba SC's
            scs_match = scs_df[scs_df[pedido_col_scs] == pedido_num]

            if not scs_match.empty:
                data_scs = pd.to_datetime(scs_match[data_col_scs].iloc[0]).date()

                # Verificar se as datas são diferentes
                if data_scs != data_saving:
                    audit_dates_results.append({
                        'Pedido': pedido_num,
                        'Data SC\'s': data_scs.strftime('%d/%m/%Y'),
                        'Data Saving': data_saving.strftime('%d/%m/%Y'),
                        'Diferença (dias)': (data_saving - data_scs).days,
                        'Status': 'DIVERGÊNCIA'
                    })
                else:
                    audit_dates_results.append({
                        'Pedido': pedido_num,
                        'Data SC\'s': data_scs.strftime('%d/%m/%Y'),
                        'Data Saving': data_saving.strftime('%d/%m/%Y'),
                        'Diferença (dias)': 0,
                        'Status': 'OK'
                    })

        if audit_dates_results:
            audit_dates_df = pd.DataFrame(audit_dates_results)

            # Separar divergências de datas
            divergencias_datas = audit_dates_df[audit_dates_df['Status'] == 'DIVERGÊNCIA']
            conformes_datas = audit_dates_df[audit_dates_df['Status'] == 'OK']

            col1, col2 = st.columns(2)

            with col1:
                st.markdown(f"**✅ Datas Conformes: {len(conformes_datas)}**")
                if not conformes_datas.empty:
                    st.markdown('<div class="audit-success">Datas consistentes!</div>', unsafe_allow_html=True)
                    st.dataframe(conformes_datas[['Pedido', 'Data SC\'s', 'Data Saving']], use_container_width=True)

            with col2:
                st.markdown(f"**⚠️ Divergências de Data: {len(divergencias_datas)}**")
                if not divergencias_datas.empty:
                    st.markdown('<div class="audit-alert">Atenção! Datas divergentes detectadas:</div>',
                                unsafe_allow_html=True)
                    st.dataframe(divergencias_datas, use_container_width=True)
                else:
                    st.markdown('<div class="audit-success">Nenhuma divergência de data encontrada!</div>',
                                unsafe_allow_html=True)

    else:
        missing_date_cols = []
        if not data_col_scs:
            missing_date_cols.append("Data na aba SC's")
        if not data_col_saving:
            missing_date_cols.append("Data na aba Saving")

        st.error(f"❌ **Auditoria de datas não disponível.**")
        st.error(f"**Colunas de data não encontradas:** {', '.join(missing_date_cols)}")

    # === RESUMO EXECUTIVO ===
    st.markdown('<div class="section-header">📈 Resumo Executivo</div>', unsafe_allow_html=True)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_pedidos = len(scs_filtered)
        st.markdown(create_kpi_card(total_pedidos, "Total de Pedidos", "number"), unsafe_allow_html=True)

    with col2:
        total_fornecedores = scs_filtered['Fornecedor'].nunique() if 'Fornecedor' in scs_filtered.columns else 0
        st.markdown(create_kpi_card(total_fornecedores, "Fornecedores Ativos", "number"), unsafe_allow_html=True)

    with col3:
        # Calcular saving percentage de forma segura
        if not saving_df.empty:
            valor_inicial_col = find_column(saving_df, ['VALOR INICIAL', 'Valor Inicial', 'Valor_Inicial'])
            saving_col = find_column(saving_df, ['Redução R$', 'Reducao R$', 'Saving', 'Economia'])

            if saving_col:
                saving_percentage = (saving_df[saving_col].sum() / scs_filtered['Valor'].sum() * 100)
            else:
                saving_percentage = 0
        else:
            saving_percentage = 0
        st.markdown(create_kpi_card(saving_percentage, "% Saving Médio", "percentage"), unsafe_allow_html=True)

    with col4:
        pedidos_com_saving = len(saving_df) if not saving_df.empty else 0
        st.markdown(create_kpi_card(pedidos_com_saving, "Pedidos c/ Saving", "number"), unsafe_allow_html=True)

    # === SEÇÃO: TOP PRODUTOS POR CATEGORIA ===
    st.markdown('<div class="section-header">🏆 Top Produtos por Categoria (Top 10)</div>', unsafe_allow_html=True)

    # Verificar se existe a coluna Categoria e Descrição
    categoria_col = find_column(scs_filtered, ['Categoria', 'CATEGORIA', 'Category'])
    descricao_col = find_column(scs_filtered, ['Descrição', 'DESCRIÇÃO', 'Descricao', 'Description', 'Produto'])

    if categoria_col and descricao_col:
        # Calcular top 10 categorias por gasto total
        top_categorias = scs_filtered.groupby(categoria_col)['Valor'].sum().reset_index()
        top_categorias = top_categorias.sort_values('Valor', ascending=False).head(10)

        st.markdown(f"#### 📊 Análise das {len(top_categorias)} categorias com maior gasto")

        # Para cada categoria do top 10, encontrar os top 5 produtos
        for idx, categoria_row in top_categorias.iterrows():
            categoria_nome = categoria_row[categoria_col]
            categoria_valor = categoria_row['Valor']

            # Filtrar produtos dessa categoria
            produtos_categoria = scs_filtered[scs_filtered[categoria_col] == categoria_nome]

            # Calcular top 5 produtos por gasto total na categoria
            top_produtos = produtos_categoria.groupby(descricao_col)['Valor'].agg(['sum', 'count']).reset_index()
            top_produtos.columns = ['Produto', 'Valor Total', 'Quantidade Pedidos']
            top_produtos = top_produtos.sort_values('Valor Total', ascending=False).head(5)

            # Calcular percentual em relação ao total da categoria
            top_produtos['% da Categoria'] = (top_produtos['Valor Total'] / categoria_valor * 100).round(1)

            # Formatar valores para exibição
            top_produtos['Valor Formatado'] = top_produtos['Valor Total'].apply(lambda x: f"R$ {x:,.2f}")
            top_produtos['% Formatado'] = top_produtos['% da Categoria'].apply(lambda x: f"{x}%")

            # Criar expander para cada categoria
            with st.expander(f"🔍 **{categoria_nome}** - Total: R$ {categoria_valor:,.2f}", expanded=False):
                # Exibir tabela dos top 5 produtos
                tabela_exibicao = top_produtos[
                    ['Produto', 'Valor Formatado', '% Formatado', 'Quantidade Pedidos']].copy()
                tabela_exibicao.columns = ['📦 Produto', '💰 Valor Total', '📊 % da Categoria', '🔢 Qtd Pedidos']

                # Resetar index para mostrar ranking
                tabela_exibicao.index = range(1, len(tabela_exibicao) + 1)

                st.dataframe(tabela_exibicao, use_container_width=True)

                # Mostrar resumo da categoria
                total_produtos_categoria = len(produtos_categoria[descricao_col].unique())
                st.markdown(f"""
                <div style="background: rgba(239, 135, 64, 0.1); padding: 0.5rem; border-radius: 5px; margin-top: 0.5rem;">
                    <small><strong>Resumo:</strong> {total_produtos_categoria} produtos únicos | 
                    Top 5 representa {top_produtos['% da Categoria'].sum():.1f}% do gasto da categoria</small>
                </div>
                """, unsafe_allow_html=True)

    else:
        missing_cols = []
        if not categoria_col:
            missing_cols.append("Categoria")
        if not descricao_col:
            missing_cols.append("Descrição/Produto")

        st.error(f"❌ **Análise não disponível.**")
        st.error(f"**Colunas não encontradas:** {', '.join(missing_cols)}")

if __name__ == "__main__":
    main()
