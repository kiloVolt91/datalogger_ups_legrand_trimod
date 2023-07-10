##DATABASE BUILDER

import mysql.connector
from mysql.connector import errorcode
import pandas as pd
from init import percorso_file_inizializzazione, file_configurazione_xls
import configparser

config = configparser.ConfigParser()
config.read(percorso_file_inizializzazione)

#dati database
db_host = config['sql_database_datalogger_energia_vm']['host']
db_user=config['sql_database_datalogger_energia_vm']['user']
db_password=config['sql_database_datalogger_energia_vm']['password']
db_database=config['sql_database_datalogger_energia_vm']['database']
db_table = 'datalogger_ups_legrand_trimod'



df = pd.read_excel(file_configurazione_xls)

colonne_registri = df.columns
colonna_nomi_parametri = colonne_registri[2]
colonna_datatype = colonne_registri[4] 
lista_parametri_db = df[colonna_nomi_parametri].tolist()
lista_parametri_db.append('data_inizio_acquisizione')
lista_parametri_db.append('data_fine_acquisizione')
lista_datatype = df[colonne_registri[4]]

mydb = mysql.connector.connect(
  host=db_host,
  user=db_user,
  password=db_password,
  database=db_database
)
mycursor = mydb.cursor()
sql_str = 'CREATE TABLE `'+str(db_database)+'`.`'+str(db_table)+'` (`id` INT NOT NULL AUTO_INCREMENT,PRIMARY KEY (`id`), UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE);'
mycursor.execute(sql_str)
mydb.commit()

i=0
for nome in lista_parametri_db[:-2]:
    sql_str = "ALTER TABLE "+str(db_table)+" ADD `" +str(nome)+ "` "+str(lista_datatype[i])
    i+=1
    mycursor.execute(sql_str)  
for nome in lista_parametri_db[-2:]:
    sql_str = "ALTER TABLE "+str(db_table)+" ADD `" +str(nome)+ "` DATETIME"
    mycursor.execute(sql_str)  

mydb.commit()
mydb.close()
