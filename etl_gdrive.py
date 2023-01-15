
# Import Library
import pandas as pd
import mysql.connector as cnn
from datetime import datetime, timedelta
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from pydrive.auth import ServiceAccountCredentials

# Write to Log File
def log(perintah):
    tanggal2 = datetime.now()
    tgl = tanggal2.strftime('%Y-%m-%d %H:%M %p')
    text_log = '{0} {1} \n'.format(tgl, perintah)
    with open('path to log file','a') as file:
        file.write(text_log)
        file.close()
        
# Load Variable
try:
    today = (datetime.now()).strftime("%Y%m%d")
    main_folder = "path to main folder"
    folder_config = main_folder+'path to config folder'
    id_url = "url.txt"
    code1 = "query.txt"
    json_acc ="path to credential google API.json"
    folder_output = "path to folder output"
    file_excel = "{0}.xlsx".format(today)
    with open(folder_config+id_url, 'r') as file_1:
        ids = file_1.read()
    with open(folder_config+code1, 'r') as file_2:
        codev = file_2.read().replace('\n',' ').replace('\t', '        ')
    log("load file sukses")
except:
    log("load file gagal")
    

# Connect to Database
try:
    conn = cnn.connect(host= "host",
                user = "user",
                password = "password",
                database = "database")
    gauth = GoogleAuth()
    scope = ['https://www.googleapis.com/auth/drive']
    gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(folder_config+json_acc, scope)
    drive = GoogleDrive(gauth)
    log("koneksi db sukses")
except:
    log("koneksi db gagal")
    

    
# Load Dataframe and transform it
try:
    df_prem = pd.read_sql(codev, conn)
    df_prem['start_date'] = pd.to_datetime(df_prem['start_date'])
    df_prem['end_date'] = pd.to_datetime(df_prem['end_date'])
    df_prem['end_date'] = df_prem['end_date'].dt.strftime("%m/%d/%Y")
    df_prem['start_date'] = df_prem['start_date'].dt.strftime("%m/%d/%Y")
    df_prem.to_excel(folder_output+file_excel,header=True,index=False)
    log("ETL data sukses")
except:
    log("ETL data gagal")
    
# Load to Gdrive
try:
    titles = "{0}.xlsx".format(today)
    gfile = drive.CreateFile({'parents': [{'id': ids}]})
    gfile['title'] = titles
    gfile.SetContentFile(filename=folder_output+file_excel)
    gfile.Upload()
    log("load {0} to gdrive sukses".format(today))
    
except:
    log("load {0} to gdrive gagal".format(today))
    
    
    
    
    
    
    
    
    
    
    
