﻿#!/usr/bin/env python
# coding: utf-8

# In[1]:


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


url = "https://talana.com/es/api/persona/?empresa=909"


headers = {
  'Authorization': 'Basic aW50ZWdyYWNpb25AdWNzaC5jbDpTb3BhaXBpbGxhSGV0ZXJvZG94YTg4',
  'Cookie': 'sessionid=b94hibz7tgd1ko69p8th2ily44tm9bk0'
}

response = requests.request("GET", url, headers=headers, data =  {})


results0  = response.text.encode('utf8')
results1 = json.loads(results0)


# Ejemplo función, mapea 2 columnas de las solicitadas
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
        "email" : registro["email"]  ,
            
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
        "detalles_direccionCiudad" : registro["detalles"][0]["direccionCiudad"],
        "tipo_contrato" : registro["empresa"]["nombre"]

  


    }

mapeado = list(map(mi_funcion, results1))
df = pd.DataFrame(mapeado)




quoted = urllib.parse.quote_plus("Driver= {SQL Server};Server=192.168.1.157;DataBase=DWHSource;UID=sa;PWD=S0portebi2@22")
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
df_talana = df.to_sql("Talana_personas_honorarios", schema='dbo', con=engine, if_exists="replace")
print("corrceto")


