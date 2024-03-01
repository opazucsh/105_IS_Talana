#!/usr/bin/env python
# coding: utf-8

# In[13]:



import pandas as pd
import numpy as np
import sys
import requests
import json
import pyodbc
from sqlalchemy import create_engine
import urllib




url = "https://talana.com/es/api/contracts-resumed?empresa=909"


payload={}
headers = {
  'Authorization': 'Basic aW50ZWdyYWNpb25AdWNzaC5jbDpTb3BhaXBpbGxhSGV0ZXJvZG94YTg4',
  'Cookie': 'sessionid=foh5l8l0gzerxcdzte53pbb3acdbwkbo'
}



response = requests.request("GET", url, headers=headers, data =  {})

results0  = response.text.encode('utf8')
results1 = json.loads(results0)



def mi_funcion(registro):
    return {    
            "id" : registro["id"],
            "persona_rut" : registro["persona_rut"] ,
            "empleado" : registro["empleado"] ,

            
            "cargo" : registro['cargo'],
            "fechaContratacion" : registro['fechaContratacion'],
            "desde" : registro['desde'],
            "hasta" : registro['hasta'],
            "finiquitado" : registro['finiquitado'],
            

            "jornada_id" : registro["jornada"]["id"],
            "jornada_nombre" : registro["jornada"]["nombre"],
            "horasDeJornada" : registro["horasDeLaJornada"],
            

            
            "persona_id" : registro['personaDetails']["id"],
            "persona_rut" : registro['personaDetails']["rut"],
            "persona_nombre" : registro['personaDetails']["nombre"],
            "persona_paterno" : registro['personaDetails']["apellidoPaterno"],
            "persona_materno" : registro['personaDetails']["apellidoMaterno"],
            "persona_sexo" : registro['personaDetails']["sexo"],
            "persona_fecha_nacimiento" : registro['personaDetails']["fechaNacimiento"],
            "persona_nacionalidad" : registro['personaDetails']["nacionalidad"],
            "persona_email" : registro['personaDetails']["email"]  ,  
            
           # 'TelefonoParticular': registro['userDefinedFields']['TelefonoParticular'] ,  
            "carrera_adscripcion" : registro['userDefinedFields']['CarreraDeAdscripcion'],
           # "secretarias" : registro['userDefinedFields']['Secretarias'],
            
           # "categoriaContable" : registro['userDefinedFields']['categoriaContable'],  
           # "tipo_academico" : registro['userDefinedFields']['TipoDeAcademico'],
            "Estamento" : registro['userDefinedFields']['Estamento'],

    
            "centro_gestion" : registro['userDefinedFields']['CentroDeGestion'],
            "CategoriaAcademica" : registro['userDefinedFields']['CategoriaAcademica'],
            
            "titulo_academico" : registro['userDefinedFields']['NombreGradoTituloObtenido'],
            'Institucion_titulo' : registro['userDefinedFields']['InstitucionTitulo'],
             "PaisDondeObtuvo" : registro['userDefinedFields']['PaisDondeObtuvo'],
             #"Ocupacion" : registro['userDefinedFields']['Ocupacion'],
            
             "activo" : registro['activo']

    }
        
mapeado1 = list(map(mi_funcion, results1))
df1 = pd.DataFrame(mapeado1)


            
result2 =[]
for i in range(0,len(results1)):
    if results1[i]['unidadOrganizacional'] != None:
        result2.append(results1[i])
        
        
        

def mi_funcion2(registro):
    return {  
            "id" : registro["id"],
            "unidadOrganizacional_nombre" : registro['unidadOrganizacional']["nombre"],
            "unidadOrganizacional_id" : registro['unidadOrganizacional']["id"],
            "unidadOrganizacional_codigo" : registro['unidadOrganizacional']["codigo"]

  
    }

mapeado2 = list(map(mi_funcion2, result2))
df2 = pd.DataFrame(mapeado2)


result3 =[]
for i in range(0,len(results1)):
    if results1[i]['centroCosto'] != None:
        result3.append(results1[i])
        
        
        

def mi_funcion3(registro):
    return {    
            "id" : registro["id"],
            "centroCosto_id" : registro["centroCosto"]["id"],
            "centroCosto_codigo" : registro["centroCosto"]["codigo"],
            "centroCosto_nombre" : registro["centroCosto"]["nombre"]

  
    }


mapeado3 = list(map(mi_funcion3, result3))
df3 = pd.DataFrame(mapeado3)

result4 =[]
for i in range(0,len(results1)):
    if results1[i]['tipoContrato'] != None:
        result4.append(results1[i])
        
def mi_funcion4(registro):
    return {    
        
            "id" : registro["id"],
            "tipoContrato_id" : registro['tipoContrato']["id"],
            "tipoContrato_nombre" : registro['tipoContrato']["nombre"]

  
    }



mapeado4 = list(map(mi_funcion4, result4))
df4 = pd.DataFrame(mapeado4)


result5 =[]
for i in range(0,len(results1)):
    if results1[i]['jefe'] != None:
        result5.append(results1[i])


def mi_funcion5(registro):
    return {    

             "id" : registro["id"],
             "jefe_id" : registro['jefe']['id'],
 
             "jefe_rut" : registro['jefe']['rut'],
             "jefe_nombre" : registro['jefe']['nombre'],
             "jefe_paterno" : registro['jefe']['apellidoPaterno'],
             "jefe_materno" : registro['jefe']['apellidoMaterno'],
             "jefe_sexo" : registro['jefe']['sexo'],
             "jefe_fechaNacimiento" : registro['jefe']['fechaNacimiento'],
             "jefe_nacionalidad" : registro['jefe']['nacionalidad'],
             "jefe_email" : registro['jefe']['email']

    }


mapeado5 = list(map(mi_funcion5, result5))
df5 = pd.DataFrame(mapeado5)


df6 = pd.merge(df1, df2, on='id', how='outer')
df7 = pd.merge(df6, df3, on='id', how='outer')
df8 = pd.merge(df7, df4, on='id', how='outer')
df9 = pd.merge(df8, df5, on='id', how='outer')



quoted = urllib.parse.quote_plus("Driver= {SQL Server};Server=192.168.1.157;DataBase=DWHSource;UID=sa;PWD=S0portebi2@22")
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
df_talana = df9.to_sql("Talana_contratos_python_resumen_h", schema='dbo', con=engine, if_exists="replace")
print("corrceto")

