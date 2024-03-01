#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import sys
import requests
import json
import pyodbc
from sqlalchemy import create_engine
import urllib


url = "https://talana.com/es/api/persona/?empresa=741"


headers = {
  'Authorization': 'Basic aW50ZWdyYWNpb25AdWNzaC5jbDpTb3BhaXBpbGxhSGV0ZXJvZG94YTg4',
  'Cookie': 'sessionid=b94hibz7tgd1ko69p8th2ily44tm9bk0'
}



response = requests.request("GET", url, headers=headers, data =  {})


results0  = response.text.encode('utf8')
results1 = json.loads(results0)



def mi_funcion(registro):
    return {    
        
        "id" : registro["id"],
        "fechaCreacion" : registro["fechaCreacion"] ,
        "rut" : registro["rut"],
        "nombre" : registro["nombre"],
        "apellido_paterno" : registro["apellidoPaterno"],
        "apellido_materno" : registro["apellidoMaterno"],
        "sexo" : registro["sexo"],
        "fecha_nacimiento" : registro["fechaNacimiento"],
        "nacionalidad" : registro["nacionalidad"],
        "userbame" : registro["username"],    
        "email" : registro["email"] 
    }

mapeado = list(map(mi_funcion, results1))
df1 = pd.DataFrame(mapeado)



result2 =[]
for i in range(0,len(results1)):
    if len(results1[i]['detalles'])>0:
        result2.append( results1[i])
        
        
def mi_funcion2(registro):
    return {    
        
        "id" : registro["id"],
        "detalles_id" : registro["detalles"][0]["id"],
        "detalles_fechaCreacion" : registro["detalles"][0]["fechaCreacion"],
        "detalles_validoDesde" : registro["detalles"][0]["validoDesde"],
        "detalles_email" : registro["detalles"][0]["email"],
        "detalles_emailPersonal" : registro["detalles"][0]["emailPersonal"],  

        "detalles_telefono" : registro["detalles"][0]["telefono"],
        "detalles_celular" : registro["detalles"][0]["celular"],
        "detalles_foto" : registro["detalles"][0]["foto"],
        "detalles_direccionCalle" : registro["detalles"][0]["direccionCalle"],
        "detalles_direccionNumero" : registro["detalles"][0]["direccionNumero"], 
        "detalles_direccionDepartamento" : registro["detalles"][0]["direccionDepartamento"],  
            
        "detalles_estadiCivil" : registro["detalles"][0]["estadoCivil"],
        "detalles_nivelEducacional" : registro["detalles"][0]["nivelEducacional"],
        "detalles_colegio" : registro["detalles"][0]["colegio"],
        "detalles_institucionEstudiosSuperiores" : registro["detalles"][0]["institucionEstudiosSuperiores"],
        "detalles_profesion" : registro["detalles"][0]["profesion"], 
        "detalles_observacion" : registro["detalles"][0]["observaciones"],  
            
        "detalles_contactosDeEmergencia" : registro["detalles"][0]["contactosDeEmergencia"],
        "detalles_direccionComuna" : registro["detalles"][0]["direccionComuna"],
        "detalles_direccionCiudad" : registro["detalles"][0]["direccionCiudad"]


    }        

mapeado2 = list(map(mi_funcion2, result2))

df2 = pd.DataFrame(mapeado2)

df3 = pd.merge(df1, df2, on='id', how='outer')

quoted = urllib.parse.quote_plus("Driver= {SQL Server};Server=192.168.1.105;DataBase=DWHSource;UID=sa;PWD=Hellyonor41")
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
df_talana = df3.to_sql("Talana_detalles_honorarios_extendido", schema='dbo', con=engine, if_exists="replace")
print("corrceto")


# In[ ]:




