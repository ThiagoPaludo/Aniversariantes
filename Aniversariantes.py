# # Importação de Bibliotecas
# pip install pyodbc
from pandas import read_csv, read_sql, DataFrame, merge, ExcelFile, concat
from datetime import datetime, timedelta, date
from os.path import isfile
from pyodbc import connect
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTP
import sys
# # Funções Personalizadas
# Função para verificar a existência de arquivos.
def ExisteArquivo(file):
    return isfile(file)  
# Função que converte datas e horas no formato desejado.
def FormataData(data, formato, delta):
    converte = {'DtHora':'%d/%m/%Y %H:%M:%S',
                'Data':'%d/%m/%Y',
                'DM':'%d/%m',
                'M':'%m',
                'Dia':'%w'
              }
    formato = converte[formato]
    wday = {'0':'Domingo',
            '1':'Segunda-feira',
            '2':'Terça-feira',
            '3':'Quarta-feira',
            '4':'Quinta-feira',
            '5':'Sexta-feira',
            '6':'Sábado'
            }
    data = data + timedelta(days=delta)
    data = data.strftime(formato)
    if formato == '%w':
        data = wday[data]
    return data
# # Definição de Variaveis Globais
# Definindo as variaveis hoje, aniver e waniver
hoje = datetime.today() #+ timedelta(days=11)
aniver = list()
waniver = {}
if FormataData(hoje, 'Dia', 0) == 'Sexta-feira':
    for c in range(0, 3):
        temp = FormataData(hoje, 'DM', c)
        aniver.append(temp)
        waniver[temp] = FormataData(hoje, 'Dia', c)
else:
    temp = FormataData(hoje, 'DM', 0)
    aniver.append(temp)
    waniver[temp] = FormataData(hoje, 'Dia', 0)
# # Verificações iniciais
# Verifica a existencia do arquivo 'Log de Envios.txt'
# Caso não exista, criar arquivo .txt
if ExisteArquivo('Log de Erros.txt') == False:  
    txt_Log = open('Log de Erros.txt', 'w')
    txt_Log.write(f'[{FormataData(hoje, "DtHora", 0)}] - Inicialização do arquivo Log.\n')
    txt_Log.close()
# Caso exista, verificar ultima data, para evitar envio em duplicidade
else:
    txt_Log = open('Log de Erros.txt', 'r')
    leitura = txt_Log.readlines()
    txt_Log.close()
# Encontrando envio de mesma data, cria msg de log e encerra a aplicação
    for l in range(0, len(leitura)):
        if leitura[l].count(f'{FormataData(hoje, "Data", 0)}') > 0 and leitura[l].count('E-mail enviado com sucesso.') > 0:
            txt_Log = open('Log de Erros.txt', 'a')
            txt_Log.write(f'[{FormataData(hoje, "DtHora", 0)}] - ERRO: Tentativa de envio em duplicidade.\n')
            txt_Log.close()
            sys.exit()
# Verifica a existencia do arquivo 'SMTP.txt'
# Caso não exista, informar em arquivo Log e encerrar a aplicação.
if ExisteArquivo('SMTP.txt') == False:
    txt_smtp = open('SMTP.txt', 'w')
    txt_smtp.write('''Parâmetros de conexão com servidor SMTP:
servidor = [smtp.servidor.com]
porta = [587]
login = [usuario@durlicouros.com.br]
senha = [senha]
remetente = [SISTEMAS DURLICOUROS]
assunto = [Aniversariantes do dia]
''')
    txt_smtp.close()
    txt_Log = open('Log de Erros.txt', 'a')
    txt_Log.write(f'[{FormataData(hoje, "DtHora", 0)}] - ERRO: Falta arquivo "SMTP.txt". \n')
    txt_Log.close()
    sys.exit()
# Verifica a existencia do arquivo 'Destinatarios.txt'
# Caso não exista, informar em arquivo Log e encerrar a aplicação.
if ExisteArquivo('Destinatarios.txt') == False:
    txt_dest = open('Destinatarios.txt', 'w')
    txt_dest.write('E-mails destinatários:\n')
    txt_dest.close()
    txt_Log = open('Log de Erros.txt', 'a')
    txt_Log.write(f'[{FormataData(hoje, "DtHora", 0)}] - ERRO: Falta arquivo "Destinatarios.txt".\n')
    txt_Log.close()
    sys.exit()
# Verifica a existencia do arquivo 'ServidorRH.txt'
# Caso não exista, informar em arquivo Log e encerrar a aplicação.
if ExisteArquivo('ServidorRH.txt') == False:
    txt_servrh = open('ServidorRH.txt', 'w')
    txt_servrh.write('''Parâmetros para acesso ao banco de dados.
server   = [192.168.1.250]
database = [Lifes_Good_2012]
username = [relatorios]
password = [relatorio]    
''')
    txt_servrh.close()
    txt_Log = open('Log de Erros.txt', 'a')
    txt_Log.write(f'[{FormataData(hoje, "DtHora", 0)}] - ERRO: Falta o arquivo "ServidorRH.txt".\n')
    txt_Log.close()
    sys.exit()
# Verifica a existencia do arquivo 'BD_PJ.xlsx'
# Caso não exista, informar em arquivo Log.
if ExisteArquivo('BD_PJ.xlsx') == False:
    txt_Log = open('Log de Erros.txt', 'a')
    txt_Log.write(f'[{FormataData(hoje, "DtHora", 0)}] - AVISO: Falta o arquivo "BD_PJ.xlsx".\n')
    txt_Log.close()
# Exit se for Sábado ou Domigo
if FormataData(hoje, 'Dia', 0) in ('Sábado', 'Domingo'):
    txt_Log = open('Log de Erros.txt', 'a')
    txt_Log.write(f'[{FormataData(hoje, "DtHora", 0)}] - ERRO: Data atual é final de semana.\n')
    txt_Log.close()
    sys.exit()
# # DataFrame Funcionarios CLT
# ## Carrega parâmetros de Servidor.txt
txt_serv = read_csv('ServidorRH.txt')
server = str(txt_serv.iloc[0,0]).strip()
start = server.find('[')+1
end = server.find(']')
server = server[start:end]
database = str(txt_serv.iloc[1,0]).strip()
start = database.find('[')+1
end = database.find(']')
database = database[start:end]
username = str(txt_serv.iloc[2,0]).strip()
start = username.find('[')+1
end = username.find(']')
username = username[start:end]
password = str(txt_serv.iloc[3,0]).strip()
start = password.find('[')+1
end = password.find(']')
password = password[start:end]
# # Conexão ODBC com servidor
try:
    cnxn = connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
except:
    txt_Log = open('Log de Erros.txt', 'a')
    txt_Log.write(f'[{FormataData(hoje, "DtHora", 0)}] - ERRO: Falha ao conectar com o servidor RH.\n')
    txt_Log.close()
    sys.exit()
# ## DF_ Funcionarios
# Tabela Funcionarios
query = "SELECT * FROM TSRS.vetorh.rh_durlicouros.r034fun"
# DataFrame Funcionarios
try:
    df_func = read_sql(query, cnxn)
    df_func = DataFrame(df_func)
    df_func = df_func[['tipcol','estcar','numemp','codfil','datadm','numcad','nomfun','datnas','codccu','codcar','sitafa']]
except:
    txt_Log = open('Log de Erros.txt', 'a')
    txt_Log.write(f'[{FormataData(hoje, "DtHora", 0)}] - ERRO: Falha ao conectar com a tabela "r034fun".\n')
    txt_Log.close()
    sys.exit()
try:
    # Variaveis de filtro
    filtro1 = df_func['tipcol'] == 1
    filtro2 = df_func['numemp'].isin([1,5,515])
    filtro3 = df_func['datadm'] <= str(hoje)
    filtro4 = df_func['estcar'] == 100
    filtro5 = ~df_func['sitafa'].isin([3,4,5,7,8,53,54,55,58,59,22])
    # Filtra os funcionarios ativos
    df_func = df_func.loc[(filtro1) & (filtro2) & (filtro3) & (filtro4) & (filtro5)]
except:
    txt_Log = open('Log de Erros.txt', 'a')
    txt_Log.write(f'[{FormataData(hoje, "DtHora", 0)}] - ERRO: Falha ao filtrar "df_func".\n')
    txt_Log.close()
    sys.exit()
# ## DF_Filiais
# Tabela Filiais
query = "SELECT * FROM TSRS.vetorh.rh_durlicouros.R030Fil"
#DataFrame Filiais
try:
    df_filial = read_sql(query, cnxn)
    df_filial = DataFrame(df_filial)
    df_filial = df_filial[['numemp', 'codfil', 'razsoc', 'nomfil']]
except:
    txt_Log = open('Log de Erros.txt', 'a')
    txt_Log.write(f'[{FormataData(hoje, "DtHora", 0)}] - ERRO: Falha ao conectar com a tabela "R030Fil".\n')
    txt_Log.close()
    sys.exit()
# ## DF_CCustos
# Tabela C.Custos
query = "SELECT * FROM TSRS.vetorh.rh_durlicouros.r018ccu"
#DataFrame C.Custos
try:
    df_cc = read_sql(query, cnxn)
    df_cc = DataFrame(df_cc)
    df_cc = df_cc[['numemp', 'codccu', 'nomccu']]
except:
    txt_Log = open('Log de Erros.txt', 'a')
    txt_Log.write(f'[{FormataData(hoje, "DtHora", 0)}] - ERRO: Falha ao conectar com a tabela "r018ccu".\n')
    txt_Log.close()
    sys.exit()
# ## DF_Cargos
# Tabela Cargos
query = "SELECT * FROM TSRS.vetorh.rh_durlicouros.R024CAR"
#DataFrame Cargos
try:
    df_cargo = read_sql(query, cnxn)
    df_cargo = DataFrame(df_cargo)
    df_cargo = df_cargo[['EstCar', 'CodCar', 'TitCar']]
except:
    txt_Log = open('Log de Erros.txt', 'a')
    txt_Log.write(f'[{FormataData(hoje, "DtHora", 0)}] - ERRO: Falha ao conectar com a tabela "R024CAR".\n')
    txt_Log.close()
    sys.exit()
# ## Relacionar as tabelas
# Cria o DataFrame df_clt referente aos funcionários cadastrados no servidor
try:
    df_clt = merge(df_func, df_filial, how = 'left', on = ['numemp','codfil'])
    df_clt = merge(df_clt, df_cc, how = 'left', on = ['numemp', 'codccu'])
    df_clt = merge(df_clt, df_cargo, how = 'left', left_on = ['estcar', 'codcar'], right_on = ['EstCar', 'CodCar'])
except :
    txt_Log = open('Log de Erros.txt', 'a')
    txt_Log.write(f'[{FormataData(hoje, "DtHora", 0)}] - ERRO: Falha ao criar o DataFrame "df_clt".\n')
    txt_Log.close()
    sys.exit()
# ## Modelagem e finalização do DataFrame (df_clt)
try:
    # Seleção de colunas relevantes
    df_clt = df_clt[['razsoc', 'nomfil', 'nomfun', 'datnas', 'codccu', 'nomccu', 'TitCar']]
    # Renomeia as colunas
    df_clt = df_clt.rename(columns={'razsoc':'Empresa', 'nomfil':'Filial', 'nomfun':'Funcionario', 'datnas':'Dt Nasc.', 
                                    'codccu':'C.Custos', 'nomccu':'Descrição', 'TitCar':'Cargo'})
    # Substitui valores da coluna 'Empresa'
    df_clt['Empresa'] = df_clt['Empresa'].apply({
        'DURLICOUROS IND COM COUROS EXP IMP LTDA':'DURLI COUROS', 
        'DURLICOUROS IND E COM DE COUROS EXP E IM':'DURLI COUROS', 
        'DURLI AGROPECUARIA S/A':'DURLI AGRO', 
        'DURLI LOGISTICA LTDA':'DURLI LOGISTICA'}.get)
    # Substitui valores da coluna 'Filial'
    df_clt['Filial'] = df_clt['Filial'].apply({
        'SÃO JOSE DOS PINHAIS':'SÃO JOSE DOS PINHAIS-PR', 
        'DURLI XINGUARA':'XINGUARA-PA', 
        'DURLI TOCANTINS':'WANDERLÂNDIA-TO', 
        'DURLI ERECHIM':'ERECHIM-RS', 
        'DURLI CUIABÁ':'CUIABÁ-MT', 
        'DURLICOUROS OBRA GALPÃO II':'SÃO JOSE DOS PINHAIS-PR', 
        'DURLICOUROS SANTA TERESINHA/BA':'SANTA TERESINHA-BA', 
        'FAZENDA SÃO FRANCISCO - XINGU':'FAZENDA SÃO FRANCISCO-MT', 
        'FAZENDA ESTRELA':'FAZENDA ESTRELA-PA', 
        'FAZENDA ALVORADA - CANARANA':'FAZENDA ALVORADA-MT', 
        'FAZENDA ESPIRITO SANTO - CAMPINÁPOLIS':'FAZENDA ESPIRITO SANTO-MT', 
        'FAZENDA BOI BRAVO':'FAZENDA BOI BRAVO-MT',
        'FAZENDA FORTALEZA':'FAZENDA FORTALEZA-MT',
        'DURLI LOGISTICA LTDA':' '}.get)
except:
    txt_Log = open('Log de Erros.txt', 'a')
    txt_Log.write(f'[{FormataData(hoje, "DtHora", 0)}] - ERRO: Falha ao modelar o DataFrame "df_clt".\n')
    txt_Log.close()
    sys.exit()
# # DataFrame Funcionarios PJ
# ## Conexão e leitura da base em Excel
try:
    # Carregando a planilha base de dados
    BaseDados = ExcelFile('BD_PJ.xlsx')
    # Seleciona a tabela da planilha
    tabela = BaseDados.parse('Cadastro')
    #Fecha o arquivo Excel
    BaseDados.close()
except:
    txt_Log = open('Log de Erros.txt', 'a')
    txt_Log.write(f'[{FormataData(hoje, "DtHora", 0)}] - ERRO: Falha ao conectar com "BD_PJ.xlsx".\n')
    txt_Log.close()
    sys.exit()
# ## Modelagem do DataFrame
try:
    # Cria o DF
    df_pj = DataFrame(tabela)
except:
    txt_Log = open('Log de Erros.txt', 'a')
    txt_Log.write(f'[{FormataData(hoje, "DtHora", 0)}] - ERRO: Falha ao criar "df_pj".\n')
    txt_Log.close()
    sys.exit()
# ## Unificar os DataFrames
try:
    # unifica as tabelas
    df_final = concat([df_clt,df_pj])
except:
    txt_Log = open('Log de Erros.txt', 'a')
    txt_Log.write(f'[{FormataData(hoje, "DtHora", 0)}] - ERRO: Falha ao concatenar os DataFrames "df_clt" e "df_pj".\n')
    txt_Log.close()
    sys.exit()
# ## Filtrar os aniversariantes do dia
try:
    # Filtra data do aniversário e ignora cargos "pensionistas"
    df_final = df_final.loc[(df_final['Cargo'] != 'Pensionista')]
    # Cria coluna data de aniversario
    df_final['Aniver'] = df_final['Dt Nasc.'].dt.strftime('%m') + df_final['Dt Nasc.'].dt.strftime('%d')
    # Formata data de nascimento
    df_final['Dt Nasc.'] = df_final['Dt Nasc.'].dt.strftime('%d/%m')
    # Ordena as colunas
    df_final = df_final.sort_values(['Filial', 'Aniver', 'Funcionario'], ascending=[True, True, True])
    # Filtra data de aniversario
    df_final = df_final.loc[(df_final['Dt Nasc.'].isin(aniver))]
    # Remove duplicados
    df_final = df_final.drop_duplicates(['Funcionario'])
except:
    txt_Log = open('Log de Erros.txt', 'a')
    txt_Log.write(f'[{FormataData(hoje, "DtHora", 0)}] - ERRO: Falha ao filtrar os aniversariantes do dia.\n')
    txt_Log.close()
    sys.exit()
# Finaliza caso não encontre aniversariantes no dia.
if df_final.shape[0] == 0:
    txt_Log = open('Log de Erros.txt', 'a')
    txt_Log.write(f'[{FormataData(hoje, "DtHora", 0)}] - AVISO: Não encontrado aniversariantes nesta data.\n')
    txt_Log.close()
    sys.exit()
# # Envio de E-mail HTML/CSS
# ## Elaboração da mensagem HTML
try:
    # Cria o primeira parte do código HTML, referente a formatação CSS
    trecho1 = '''
    <!DOCTYPE html>
    <html>
        <head>
            <style>
                body {
                    width: 940px;
                    font-family: "Helvetica", "Lucida Grande", sans-serif;
                    text-align: left;
                    font-size: 16px;
                    color: #3D3D3D;
                    margin-left: auto;
                    margin-right: auto;
                    }

                ul {
                    padding-bottom: 16px;
                    }

                li {
                    font-family: "Helvetica", "Lucida Grande", sans-serif;
                    text-align: left;
                    font-size: 16px;
                    color: #3D3D3D;
                    margin-left: auto;
                    margin-right: auto;
                    }

                h1 {
                    text-align: left;
                    font-size: 24px;
                    text-align: center;
                    color: black;
                    font-weight: bold;
                    border-bottom: 1px solid #000000;
                    padding: 10px;
                    background-color: #E1EDF2;
                    }

                h3 {
                    text-align: left;
                    font-size: 16px;
                    color: black;
                    font-weight: bold;
                    border-bottom: 0px solid #000000;
                    padding-left: 10px;
                    background-color: #E1EDF2;
                    }
            </style>
        </head>
        <body>
    '''
    # Cria o titulo da mensagem, contendo a data atual
    if FormataData(hoje, 'Dia', 0) == 'Sexta-feira':
        trecho2 = f'''        <h1>ANIVERSARIANTES DO FINAL DE SEMANA ({FormataData(hoje, 'Data', 0)})</h1>\n'''
    else:
        trecho2 = f'''        <h1>ANIVERSARIANTES DO DIA ({FormataData(hoje, 'Data', 0)})</h1>\n'''
    # Faz o loop para criar o corpo da mensagem, contendo a empresa, as filiais, nomes e cargos
    empresas = df_final['Empresa'].unique()
    trecho3 = ''
    for le in empresas:
        df_copia = df_final.loc[df_final['Empresa']==le]
        filiais = sorted(df_copia['Filial'].unique())
        for lf in filiais:
            trecho3 += f'''        <h3>{le} / {lf}</h3>\n'''
            trecho3 += '''        <ul>\n'''
            df_copia = df_final.loc[(df_final['Empresa']==le) & (df_final['Filial']==lf)]
            for l in range(0, df_copia.shape[0]):
                if FormataData(hoje, 'Dia', 0) != 'Sexta-feira':
                    trecho3 += f'''            <li>{str(df_copia.iloc[l][2]).title()} ({str(df_copia.iloc[l][6])})</li>\n'''
                else:
                    if df_copia.iloc[l][3] == FormataData(hoje, 'DM', 0):
                        trecho3 += f'''            <li>[Hoje] - {str(df_copia.iloc[l][2]).title()} ({str(df_copia.iloc[l][6])})</li>\n'''
                    else:
                        trecho3 += f'''            <li>[{waniver[df_copia.iloc[l][3]]}] - {str(df_copia.iloc[l][2]).title()} ({str(df_copia.iloc[l][6])})</li>\n'''
            trecho3 += '''        </ul>\n'''
    # Faz o fechamento do código HTML
    trecho4 = '''    </body>
    </html>
    '''
    # Reune todos os segmentos da mensagem HTMl
    mensagem = trecho1 + trecho2 + trecho3 + trecho4
except:
    txt_Log = open('Log de Erros.txt', 'a')
    txt_Log.write(f'[{FormataData(hoje, "DtHora", 0)}] - ERRO: Falha ao criar mensagem HTML.\n')
    txt_Log.close()
    sys.exit()
# ## Carregar endereços de e-mail do arquivo .txt
try:
    # Faz a leitura do arquivo texto
    txt_emails = read_csv('Destinatarios.txt')
    # Carrega string com os endereços de e-mails
    destinatarios = ''
    l = txt_emails.shape[0]
    for n in range(0, l):
        destinatarios = destinatarios + str(txt_emails.iloc[n,0]).strip().lower()
        if n != l-1:
            destinatarios = destinatarios + ', '
except:
    txt_Log = open('Log de Erros.txt', 'a')
    txt_Log.write(f'[{FormataData(hoje, "DtHora", 0)}] - ERRO: Falha ao carregar e-mails dos destinatários.\n')
    txt_Log.close()
    sys.exit()
# ## Conexão com o servidor SMTP
txt_smtp = read_csv('SMTP.txt')
servidor = str(txt_smtp.iloc[0,0]).strip()
start = servidor.find('[')+1
end = servidor.find(']')
servidor = servidor[start:end]
porta = str(txt_smtp.iloc[1,0]).strip()
start = porta.find('[')+1
end = porta.find(']')
porta = porta[start:end]
login = str(txt_smtp.iloc[2,0]).strip()
start = login.find('[')+1
end = login.find(']')
login = login[start:end]
senha = str(txt_smtp.iloc[3,0]).strip()
start = senha.find('[')+1
end = senha.find(']')
senha = senha[start:end]
remetente = str(txt_smtp.iloc[4,0]).strip()
start = remetente.find('[')+1
end = remetente.find(']')
remetente = remetente[start:end]
assunto = str(txt_smtp.iloc[5,0]).strip()
start = assunto.find('[')+1
end = assunto.find(']')
assunto = assunto[start:end]
# ## Envio do e-mail
# Preenche informações basicas
mimemsg = MIMEMultipart()
mimemsg['From'] = remetente
mimemsg['To'] = 'aniversariantes@durlicouros.com.br'
mimemsg['Bcc'] = destinatarios
mimemsg['Subject'] = assunto
# Preenche o corpo do e-mail
mimemsg.attach(MIMEText(mensagem, 'html'))
# Faz a conexão e envia o e-mail
try:    
    connection = SMTP(host=servidor, port=porta)
    connection.starttls()
    connection.login(login, senha)
    connection.send_message(mimemsg)
    # escreve mensagem de envio com sucesso no arquivo log
    txt_Envios = open('Log de Erros.txt', 'a')
    txt_Envios.write(f'[{FormataData(hoje, "DtHora", 0)}] - E-mail enviado com sucesso.\n')
    txt_Envios.close()
except:
    # escreve mensagem de falha no arquivo log
    txt_Envios = open('Log de Erros.txt', 'a')
    txt_Envios.write(f'[{FormataData(hoje, "DtHora", 0)}] - ERRO: Falha ao enviar o e-mail.\n')
    txt_Envios.close()
finally:    
    connection.quit()