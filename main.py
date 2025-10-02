import streamlit as st
import yaml
import pandas as pd
import json
from datetime import datetime
import io
import os
from streamlit_option_menu import option_menu

# Configuração da página
st.set_page_config(
    page_title="Editor YAML para DAGs do Airflow",
    page_icon="📊",
    layout="wide"
)

# Título da aplicação
st.title("📊 Editor YAML para DAGs do Airflow")

# Carregar configurações do arquivo JSON
def carregar_configuracoes():
    """
    Carrega as configurações do arquivo config.json
    Se o arquivo não existir, cria uma configuração padrão
    """
    try:
        with open('config.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        st.error(f"Erro ao carregar configurações: {e}")
        return None

# Carregar configurações
CONFIG = carregar_configuracoes()

if CONFIG is None:
    st.stop()

# Template de DAG vazia
DAG_TEMPLATE = {
    'dag': {
        'descricao': '',
        'dono': '',
        'email_em_falha': 'rodrigo.silva@ext.saint-gobain.com',
        'data_inicial': datetime.now().strftime('%Y-%m-%d'),
        'agendamento_cron': '0 0 * * *',
        'tags': '',
        'quantidade_tentativas': 2,
        'tempo_para_tentativa': 1,
        'ordem_execução': ''
    },
    'tasks': []
}

# Inicialização do estado da sessão
if 'dag_data' not in st.session_state:
    st.session_state.dag_data = None

if 'current_task' not in st.session_state:
    st.session_state.current_task = {}

if 'original_filename' not in st.session_state:
    st.session_state.original_filename = ""

if 'editing_task_index' not in st.session_state:
    st.session_state.editing_task_index = None

if 'form_cleared' not in st.session_state:
    st.session_state.form_cleared = False

if 'modo_atual' not in st.session_state:
    st.session_state.modo_atual = "Criar Parametro DAG"

if 'arquivo_carregado' not in st.session_state:
    st.session_state.arquivo_carregado = False

# Funções auxiliares
def criar_nova_dag():
    """Cria uma nova DAG do zero"""
    st.session_state.dag_data = DAG_TEMPLATE.copy()
    st.session_state.original_filename = "nova_dag.yml"
    st.session_state.modo_atual = "Criar Parametro DAG"
    st.session_state.arquivo_carregado = True

def resetar_aplicacao():
    """Reseta toda a aplicação para o estado inicial"""
    st.session_state.dag_data = None
    st.session_state.current_task = {}
    st.session_state.original_filename = ""
    st.session_state.editing_task_index = None
    st.session_state.form_cleared = False
    st.session_state.arquivo_carregado = False

def load_yaml_file(uploaded_file):
    """Carrega e valida o arquivo YAML"""
    try:
        content = uploaded_file.read().decode('utf-8')
        dag_data = yaml.safe_load(content)
        
        # Validação básica da estrutura
        if not isinstance(dag_data, dict):
            st.error("O arquivo YAML deve ser um dicionário")
            return None
            
        if 'dag' not in dag_data or 'tasks' not in dag_data:
            st.error("O arquivo YAML deve conter as seções 'dag' e 'tasks'")
            return None
            
        st.session_state.original_filename = uploaded_file.name
        st.session_state.modo_atual = "Editar Parametro DAG"
        st.session_state.arquivo_carregado = True
        return dag_data
        
    except yaml.YAMLError as e:
        st.error(f"Erro ao ler arquivo YAML: {e}")
        return None
    except Exception as e:
        st.error(f"Erro ao processar arquivo: {e}")
        return None

def inserir_task_abaixo(index):
    """Insere uma nova task abaixo da task especificada"""
    st.session_state.current_task = {}
    st.session_state.editing_task_index = index + 1
    st.session_state.inserir_abaixo = True

def add_or_update_task():
    """Adiciona uma nova task ou atualiza uma existente"""
    if st.session_state.current_task:
        new_task = st.session_state.current_task.copy()
        
        if st.session_state.editing_task_index is not None:
            if getattr(st.session_state, 'inserir_abaixo', False):
                # Inserir nova task abaixo da task atual
                new_task['task_num'] = st.session_state.editing_task_index
                st.session_state.dag_data['tasks'].insert(st.session_state.editing_task_index, new_task)
                # Reorganizar números das tasks
                for i, task in enumerate(st.session_state.dag_data['tasks']):
                    task['task_num'] = i
                st.success("Task inserida com sucesso!")
                st.session_state.inserir_abaixo = False
            else:
                # Atualizar task existente
                st.session_state.dag_data['tasks'][st.session_state.editing_task_index] = new_task
                st.success("Task atualizada com sucesso!")
        else:
            # Adicionar nova task no final
            new_task['task_num'] = len(st.session_state.dag_data['tasks'])
            st.session_state.dag_data['tasks'].append(new_task)
            st.success("Task adicionada com sucesso!")
        
        # Limpar formulário automaticamente
        clear_form()
        st.session_state.form_cleared = True

def remove_task(task_num):
    """Remove uma task da lista"""
    st.session_state.dag_data['tasks'] = [task for task in st.session_state.dag_data['tasks'] if task['task_num'] != task_num]
    # Reorganiza os números das tasks
    for i, task in enumerate(st.session_state.dag_data['tasks']):
        task['task_num'] = i
    st.success("Task removida com sucesso!")

def download_yaml():
    """Prepara o arquivo YAML para download"""
    yaml_str = yaml.dump(st.session_state.dag_data, default_flow_style=False, allow_unicode=True, sort_keys=False)
    return yaml_str

def edit_task(task_index):
    """Prepara o formulário para edição de uma task existente"""
    task = st.session_state.dag_data['tasks'][task_index]
    st.session_state.current_task = task.copy()
    st.session_state.editing_task_index = task_index

def clear_form():
    """Limpa o formulário de task"""
    st.session_state.current_task = {}
    st.session_state.editing_task_index = None
    if hasattr(st.session_state, 'inserir_abaixo'):
        del st.session_state.inserir_abaixo

# Menu de opções superior
selected = option_menu(
    None,
    ["Criar Parametro DAG", "Editar Parametro DAG"],
    icons=['plus-circle', 'pencil-square'],
    menu_icon="cast",
    default_index=0,
    orientation="horizontal",
    key='menu_principal'
)

# Atualizar modo baseado na seleção do menu
if selected != st.session_state.modo_atual:
    st.session_state.modo_atual = selected
    if selected == "Criar Parametro DAG":
        criar_nova_dag()
    else:
        resetar_aplicacao()

# Conteúdo baseado no modo selecionado
if st.session_state.modo_atual == "Criar Parametro DAG":
    st.markdown("### 🆕 Criar Nova DAG")
    
    # Inicializar DAG vazia se não existir
    if st.session_state.dag_data is None:
        criar_nova_dag()

elif st.session_state.modo_atual == "Editar Parametro DAG":
    st.markdown("### 📁 Editar DAG Existente")
    
    if not st.session_state.arquivo_carregado:
        # Upload do arquivo para edição
        uploaded_file = st.file_uploader(
            "📤 Anexe seu arquivo YAML para editar", 
            type=['yml', 'yaml'],
            help="Selecione um arquivo YAML no formato das DAGs do Airflow"
        )
        
        if uploaded_file is not None:
            st.session_state.dag_data = load_yaml_file(uploaded_file)
            if st.session_state.dag_data:
                st.success(f"✅ Arquivo '{st.session_state.original_filename}' carregado com sucesso!")
                st.rerun()
            else:
                st.error("❌ Erro ao carregar o arquivo. Verifique o formato.")
    else:
        # Mostrar informações do arquivo carregado
        st.info(f"📄 Editando: **{st.session_state.original_filename}**")

# Se há uma DAG carregada (em qualquer modo), mostrar TODAS as seções
if st.session_state.dag_data is not None and st.session_state.arquivo_carregado:
    
    # Seção DAG - Configurações (APARECE EM AMBOS OS MODOS)
    st.header("⚙️ Configurações da DAG")
    
    with st.container():
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.session_state.dag_data['dag']['descricao'] = st.text_input(
                "Descrição*",
                value=st.session_state.dag_data['dag'].get('descricao', ''),
                placeholder="Descrição da DAG"
            )
            
            st.session_state.dag_data['dag']['dono'] = st.text_input(
                "Dono*", 
                value=st.session_state.dag_data['dag'].get('dono', ''),
                placeholder="Nome do responsável"
            )
            
            st.session_state.dag_data['dag']['email_em_falha'] = st.text_input(
                "Email em Falha*", 
                value=st.session_state.dag_data['dag'].get('email_em_falha', ''),
                placeholder="email1@exemplo.com,email2@exemplo.com"
            )
        
        with col2:
            data_inicial = st.session_state.dag_data['dag'].get('data_inicial', datetime.now().strftime('%Y-%m-%d'))
            try:
                date_value = datetime.strptime(data_inicial, '%Y-%m-%d')
            except:
                date_value = datetime.now()
            
            st.session_state.dag_data['dag']['data_inicial'] = st.date_input(
                "Data Inicial*", 
                value=date_value
            ).strftime('%Y-%m-%d')
            
            st.session_state.dag_data['dag']['agendamento_cron'] = st.text_input(
                "Agendamento Cron*", 
                value=st.session_state.dag_data['dag'].get('agendamento_cron', ''),
                placeholder="0 0 * * *",
                help="""
                        ┌───────────── minuto (0 - 59)
                        │ ┌───────────── hora (0 - 23)
                        │ │ ┌───────────── dia do mês (1 - 31)
                        │ │ │ ┌───────────── mês (1 - 12)
                        │ │ │ │ ┌───────────── dia da semana (0 - 6) (0=Domingo)
                        │ │ │ │ │
                        * * * * *
                    """
            )
            
            st.session_state.dag_data['dag']['tags'] = st.text_input(
                "Tags", 
                value=st.session_state.dag_data['dag'].get('tags', ''),
                placeholder="TAG1,TAG2"
            )
        
        with col3:
            st.session_state.dag_data['dag']['quantidade_tentativas'] = st.number_input(
                "Quantidade de Tentativas*", 
                min_value=0, 
                value=st.session_state.dag_data['dag'].get('quantidade_tentativas', 2)
            )
            
            st.session_state.dag_data['dag']['tempo_para_tentativa'] = st.number_input(
                "Tempo para Tentativa*", 
                min_value=0, 
                value=st.session_state.dag_data['dag'].get('tempo_para_tentativa', 1)
            )
            
            st.session_state.dag_data['dag']['ordem_execução'] = st.text_area(
                "Ordem de Execução", 
                value=st.session_state.dag_data['dag'].get('ordem_execução', ''),
                placeholder="Ordem de execução das tasks...",
                height=100
            )

    # Seção Tasks (APARECE EM AMBOS OS MODOS)
    st.header("📋 Gerenciar Tasks")
    
    # Formulário para adicionar/editar tasks
    with st.form("task_form"):
        editing_mode = st.session_state.editing_task_index is not None
        form_title = "✏️ Editando Task" if editing_mode else "➕ Adicionar Nova Task"
        st.subheader(form_title)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.session_state.current_task['identificador_task'] = st.text_input(
                "Identificador da Task*",
                value=st.session_state.current_task.get('identificador_task', ''),
                placeholder="minha_task"
            )
            
            st.session_state.current_task['ativo'] = st.selectbox(
                "Ativo*",
                options=[1, 0],
                index=0 if st.session_state.current_task.get('ativo', 1) == 1 else 1,
                format_func=lambda x: "Sim" if x == 1 else "Não"
            )
            
            # Dropdown para ID da Conexão
            conexao_atual = st.session_state.current_task.get('conexao_id', '')
            if conexao_atual and conexao_atual not in CONFIG['conexoes']:
                CONFIG['conexoes'].append(conexao_atual)
            
            st.session_state.current_task['conexao_id'] = st.selectbox(
                "ID da Conexão*",
                options=CONFIG['conexoes'],
                index=CONFIG['conexoes'].index(conexao_atual) if conexao_atual in CONFIG['conexoes'] else 0
            )
        
        with col2:
            # Dropdown para Mini Operador
            operador_atual = st.session_state.current_task.get('mini_operador', '')
            st.session_state.current_task['mini_operador'] = st.selectbox(
                "Mini Operador*",
                options=CONFIG['mini_operadores'],
                index=CONFIG['mini_operadores'].index(operador_atual) if operador_atual in CONFIG['mini_operadores'] else 0
            )
            
            st.session_state.current_task['id_mini_operador'] = st.text_input(
                "ID do Mini Operador*",
                value=st.session_state.current_task.get('id_mini_operador', ''),
                placeholder="c36213a0-8322-11ee-b566-537f0445863c"
            )
            
            st.session_state.current_task['extra_info'] = st.text_area(
                "Informações Extras",
                value=st.session_state.current_task.get('extra_info', ''),
                placeholder="Informações adicionais...",
                height=100
            )
        
        # Botões do formulário
        col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 2])
        with col_btn1:
            submit_label = "💾 Atualizar Task" if editing_mode else "✅ Adicionar Task"
            form_submitted = st.form_submit_button(submit_label)
        with col_btn2:
            if st.form_submit_button("🗑️ Limpar Formulário"):
                clear_form()
                st.session_state.form_cleared = True
                st.rerun()
        
        if form_submitted:
            if st.session_state.current_task.get('identificador_task'):
                add_or_update_task()
                clear_form()
                st.rerun()
            else:
                st.error("Por favor, preencha pelo menos o 'Identificador da Task'")

    # Tabela de tasks existentes - Layout Compacto
    st.subheader("📊 Tasks Configuradas")
    
    if st.session_state.dag_data['tasks']:
        # CSS para linhas mais compactas
        st.markdown("""
        <style>
        .compact-row {
            line-height: 1.0 !important;
            min-height: 20px !important;
            padding: 1px 0px !important;
        }
        .small-text {
            font-size: 12px !important;
        }
        </style>
        """, unsafe_allow_html=True)

        columns_widths = [0.9, 0.3, 0.3, 1.5, 1.2, 0.8, 2]
        
        # Cabeçalho
        cols = st.columns(columns_widths)
        with cols[0]:
            st.markdown("**Ações**")
        with cols[1]:
            st.markdown("**Nº**")
        with cols[2]:
            st.markdown("**Ativo**")
        with cols[3]:
            st.markdown("**Identificador**")
        with cols[4]:
            st.markdown("**Conexão**")
        with cols[5]:
            st.markdown("**Operador**")
        with cols[6]:
            st.markdown("**Detalhes**")
        
        st.divider()
        
        # Linhas das tasks - ALTURA REDUZIDA
        for i, task in enumerate(st.session_state.dag_data['tasks']):
            # Container compacto para cada linha
            with st.container():
                cols = st.columns(columns_widths)
                
                with cols[0]:
                    # Botões de ação compactos - mesma linha
                    btn_col1, btn_col2, btn_col3 = st.columns(3)
                    
                    with btn_col1:
                        if st.button("⬇️", key=f"insert_{i}"):
                            inserir_task_abaixo(i)
                            st.rerun()
                    
                    with btn_col2:
                        if st.button("✏️", key=f"edit_{i}"):
                            edit_task(i)
                            st.rerun()
                    
                    with btn_col3:
                        if st.button("🗑️", key=f"delete_{i}"):
                            remove_task(task['task_num'])
                            st.rerun()
                
                with cols[1]:
                    st.markdown(f"<div class='compact-row small-text'>**{task['task_num']}**</div>", unsafe_allow_html=True)
                
                with cols[2]:
                    st.markdown(f"<div class='compact-row'>✅</div>" if task['ativo'] == 1 else "<div class='compact-row'>❌</div>", unsafe_allow_html=True)
                
                with cols[3]:
                    st.markdown(f"<div class='compact-row small-text'>`{task['identificador_task']}`</div>", unsafe_allow_html=True)
                
                with cols[4]:
                    st.markdown(f"<div class='compact-row small-text'>`{task['conexao_id']}`</div>", unsafe_allow_html=True)
                
                with cols[5]:
                    st.markdown(f"<div class='compact-row small-text'>`{task['mini_operador']}`</div>", unsafe_allow_html=True)
                
                with cols[6]:
                    # Expander para detalhes (ID Operador e Extra Info) - APENAS O BOTÃO
                    with st.expander("🔍"):
                        st.write(f"**ID do Mini Operador:**")
                        # Converter para string para evitar erro de len()
                        id_operador = str(task['id_mini_operador'])
                        st.code(id_operador)
                        
                        extra_info = task.get('extra_info', '')
                        if extra_info:
                            st.write(f"**Informações Extras:**")
                            st.code(extra_info)
                        else:
                            st.info("Nenhuma informação extra")
            
            # Linha divisória sutil entre tasks (exceto para a última)
            if i < len(st.session_state.dag_data['tasks']) - 1:
                st.divider()
    else:
        st.info("Nenhuma task configurada. Adicione tasks usando o formulário acima.")

    # Botão de download
    st.header("💾 Download do Arquivo YAML")
    yaml_output = download_yaml()
    
    if st.session_state.modo_atual == "Criar Parametro DAG":
        nome_arquivo = f"{st.session_state.current_task.get('identificador_task', '')}.yml"
    else:
        nome_arquivo = f"{st.session_state.original_filename}"
    
    st.download_button(
        label="📥 Baixar Arquivo YAML",
        data=yaml_output,
        file_name=nome_arquivo,
        mime="application/x-yaml",
        use_container_width=True
    )

    # Visualização do YAML (opcional)
    with st.expander("👁️ Visualização do YAML"):
        st.code(yaml_output, language='yaml')

# Informações de ajuda
with st.expander("ℹ️ Ajuda e Informações"):
    st.markdown("""
    ### Como Usar:
    
    #### 🆕 **Criar Parametro DAG:**
    - Selecione **"Criar Parametro DAG"** no menu superior
    - Preencha todas as configurações da DAG (campos com * são obrigatórios)
    - Adicione tasks usando o formulário
    - Baixe o arquivo YAML final
    
    #### 📁 **Editar Parametro DAG:**
    - Selecione **"Editar Parametro DAG"** no menu superior
    - Anexe um arquivo YAML existente
    - **AS CONFIGURAÇÕES DA DAG SERÃO CARREGADAS AUTOMATICAMENTE**
    - Edite as configurações e tasks conforme necessário
    - Baixe o arquivo YAML editado
    
    #### 📋 **Gerenciar Tasks:**
    - Use **⬇️** para inserir uma task após outra específica
    - Use **✏️** para modificar uma task existente  
    - Use **🗑️** para remover uma task
    - Use **🔍** para ver ID do Operador e Informações Extras completas
    
    ### Configurações:
    - As opções de **ID da Conexão** e **Mini Operadores** são carregadas do arquivo `config.json`
    - Você pode editar o arquivo `config.json` para adicionar novas opções
    
    ### Campos Obrigatórios (*):
    - **Descrição**: Descrição da DAG
    - **Dono**: Responsável pela DAG
    - **Email em Falha**: Emails para notificação
    - **Data Inicial**: Data de início da DAG
    - **Agendamento Cron**: Expressão cron para agendamento
    - **Quantidade de Tentativas**: Número de tentativas em caso de falha
    - **Tempo para Tentativa**: Tempo entre tentativas
    - **Identificador da Task**: Nome único para a task
    - **Ativo**: Se a task está ativa
    - **ID da Conexão**: ID da conexão no Airflow
    - **Mini Operador**: Tipo de operador
    - **ID do Mini Operador**: ID específico do operador
    """)