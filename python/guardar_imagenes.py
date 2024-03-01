
import pandas as pd
import numpy as np
import sqlalchemy
import requests



import pyodbc

conn = pyodbc.connect ("Driver= {SQL Server};Server=192.168.1.157;DataBase=DWHSource;")
cursor = conn.cursor()

df_total = pd.read_sql("SELECT rut, foto FROM [dbo].[Talana_personal_tc] WHERE  foto is not null",conn)

listas = df_total.to_numpy().tolist()


for elemento in listas :     
        f = open('G:/fotos_administrativos/'+elemento[0]+'.jpg','wb')
        response = requests.get(elemento[1])
        f.write(response.content)
        f.close()
        
print("download successful")

