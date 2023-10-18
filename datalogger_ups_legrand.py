###LETTURA MODBUS UPS LEGRAND TRIMOD

import configparser
import sys
import pandas as pd
from datetime import datetime
from pyModbusTCP.client import ModbusClient
from pyModbusTCP import utils
import mysql.connector
from mysql.connector import errorcode
import time
from init import percorso_file_inizializzazione, file_configurazione_xls


def inizializzazione_dati():
    global dict_id_impianti
    global file_configurazione_xls
    global percorso_file_inizializzazione
    global config
    config = configparser.ConfigParser()
    config.read(percorso_file_inizializzazione)
    id1 = config['datalogger_ups_trimod_1']['nome_impianto']
    id2 = config['datalogger_ups_trimod_2']['nome_impianto']
    id3 = config['datalogger_ups_trimod_3']['nome_impianto']
    dict_id_impianti = {1:id1, 2:id2, 3:id3}
    return 

def seleziona_identificativo():
    for identificativo in list(dict_id_impianti.keys()):
        print(identificativo, '] ', dict_id_impianti[identificativo])
        c=0
    while True:
        ans = input("Selezionare l'impianto per cui estrarre i dati")
        if ans.isnumeric() == False:
            print('Il valore inserito non è un numero')
            c+=1
        else: 
            if int(ans) in list(dict_id_impianti.keys()):
                identificativo = int(ans)
                break
            c+=1
            print('è stato inserito un valore errato, riprovare')
        if c == 10:
            sys.exit('numero di tentativi esaurito')
    return(identificativo)

def inizializzazione_parametri(identificativo):
    global modbus_server_host
    global modbus_server_port
    global db_host
    global db_user
    global db_password
    global db_database
    global db_table

    modbus_server_host = config['datalogger_ups_trimod_'+str(identificativo)]['host']
    modbus_server_port = config['datalogger_ups_trimod_'+str(identificativo)]['port']
    modbus_nome_impianto = config['datalogger_ups_trimod_'+str(identificativo)]['nome_impianto']
    if dict_id_impianti[identificativo] != modbus_nome_impianto:
        sys.exit("L'identificativo impianto non è coerente con i dati di configurazione del database") 
    db_host = config['sql_database_datalogger_energia_vm']['host']
    db_user=config['sql_database_datalogger_energia_vm']['user']
    db_password=config['sql_database_datalogger_energia_vm']['password']
    db_database=config['sql_database_datalogger_energia_vm']['database']
    db_table = 'datalogger_ups_legrand_trimod'
    return

def obtain_registers_informations():
    df = pd.read_excel(file_configurazione_xls)
    colonne_configurazione= df.columns
    colonna_nomi_parametri = colonne_configurazione[2]
    colonna_registro_hex= colonne_configurazione[0] 
    colonna_word = colonne_configurazione[3] 
    colonna_datatype = colonne_configurazione[4] 
    global lista_parametri_db
    global lista_datatype
    lista_parametri_db = df[colonna_nomi_parametri].tolist()
    lista_parametri_db.append('data_inizio_acquisizione')
    lista_parametri_db.append('data_fine_acquisizione')
    lista_parametri_db.append('fk_id_impianto')
    lista_datatype = df[colonne_configurazione[4]]
    indirizzi_holding_registers = []
    numero_word_holding_registers = []
    for i in range (0, df.shape[0]):
        indirizzi_holding_registers.append(df[colonna_registro_hex][i])
        numero_word_holding_registers.append(df[colonna_word][i])
    return(indirizzi_holding_registers, numero_word_holding_registers)

def data_extraction_routine(identificativo, indirizzi_holding_registers, numero_word_holding_registers):
    t0 = datetime.now()
    data_row =[]
    mb_cnx = modbus_connection(modbus_server_host, modbus_server_port)
    for i in range (0, len(indirizzi_holding_registers)):
        lettura_holding_reg = mb_cnx.read_holding_registers(indirizzi_holding_registers[i], numero_word_holding_registers[i])
        data_row.append(lettura_holding_reg[0])
    t1 = datetime.now()
    data_row.append(t0)
    data_row.append(t1)
    data_row.append(identificativo)
    to_database = pd.Series(index=lista_parametri_db, data = data_row)
    sql_cnx = mysql_connection(db_host, db_user, db_password, db_database)
    sql_upload_df(to_database, db_table, sql_cnx)
    print('DL UPS Legrand.Trimod - Upload db eseguito per: '+str(dict_id_impianti[identificativo])+', durata estrazione: ',(t1-t0).total_seconds(), ' s')
    time.sleep(10)
    return

def sql_upload_df(dataframe_valori, db_table, sql_cnx): 
    colonne = dataframe_valori.index.tolist()
    placeholders = '%s'
    str_nomi = '(`'+colonne[0]+'`,'
    str_vals = '(%s,'
    for i in range(1, len(colonne)):
        if i == len(colonne)-1:
            str_nomi = str_nomi +'`'+ colonne[i] +'`' +')'
            str_vals = str_vals + placeholders + ')'
        else: 
            str_nomi = str_nomi +'`'+ colonne[i] +'`'+', '
            str_vals = str_vals + placeholders + ', '
    mysql_str = "INSERT INTO "+ db_table+ " {col_name} VALUES {values}".format(col_name = str_nomi, values = str_vals)
    cursor = sql_cnx.cursor()
    cursor.execute (mysql_str, dataframe_valori.tolist())
    sql_cnx.commit()
    cursor.close()
    sql_cnx.close()
    return

def modbus_connection(modbus_server_host, modbus_server_port):
    try:
        mb_cnx = ModbusClient(host = modbus_server_host, port =int(modbus_server_port), timeout=5)
    except ValueError:
        print("Parametri ':"+str(modbus_server_host)+str(modbus_server_port)+"' errati")
    return (mb_cnx)

def mysql_connection(db_host, db_user, db_password, db_database):
    try:
        sql_cnx = mysql.connector.connect(host=db_host, user=db_user, password=db_password, database=db_database)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Combinazione nome utente-password errata")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Il database: '"+str(database)+"' non esiste")
        else:
            print(err)
    return(sql_cnx)

def datalogger_ups():
    inizializzazione_dati()
    identificativo = seleziona_identificativo()
    inizializzazione_parametri(identificativo)

    indirizzi_holding_registers, numero_word_holding_registers = obtain_registers_informations()
    while True:   
        data_extraction_routine(identificativo, indirizzi_holding_registers, numero_word_holding_registers)
    return

while True:
    try:
        datalogger_ups()
    except Exception as error:
        print(datetime.now())
        print('Errore :')
        sys.exit(str(error))
    break
