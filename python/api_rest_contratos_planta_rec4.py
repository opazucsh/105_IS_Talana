#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!/usr/bin/env python
# coding: utf-8

# In[17]:





import pandas as pd
import numpy as np
import sys
import requests
import json
import pyodbc
from sqlalchemy import create_engine
import urllib




url = "https://talana.com/es/api/contrato/"


headers = {  'Authorization': 'Basic aW50ZWdyYWNpb25AdWNzaC5jbDpTb3BhaXBpbGxhSGV0ZXJvZG94YTg4',
  'Cookie': 'sessionid=qgm8r88pr6j1qq1zsm6ijrw5nb430eay'}

response = requests.request("GET", url, headers=headers, data =  {})

results0  = response.text.encode('utf8')
results1 = json.loads(results0)



def mi_funcion(registro):
    return {    "id" : registro["id"],
        "empleado" : registro["empleado"] ,
        "empleado_id" : registro['empleadoDetails']["id"],
        "empleado_rut" : registro['empleadoDetails']["rut"],
        "empleado_nombre" : registro['empleadoDetails']["nombre"],
        "empleado_paterno" : registro['empleadoDetails']["apellidoPaterno"],
        "empleado_materno" : registro['empleadoDetails']["apellidoMaterno"],
        "empleado_sexo" : registro['empleadoDetails']["sexo"],
        "empleado_fecha_nacimiento" : registro['empleadoDetails']["fechaNacimiento"],
        "emplead_nacionalidad" : registro['empleadoDetails']["nacionalidad"],
        "empleado_email" : registro['empleadoDetails']["email"]  ,       
        "detalles_empleado_id" : registro['empleadoDetails']['detalles'][0]["id"],
        "detalles_empleado_mail_personal" : registro['empleadoDetails']['detalles'][0]["emailPersonal"],
        "detalles_empleado_telefono" : registro['empleadoDetails']['detalles'][0]["telefono"],
        "detalles_empleado_celular" : registro['empleadoDetails']['detalles'][0]["celular"],
        "detalles_empleado_direccioncalle" : registro['empleadoDetails']['detalles'][0]["direccionCalle"],
        "detalles_empleado_direccionnumero" : registro['empleadoDetails']['detalles'][0]["direccionNumero"],
        "detalles_empleado_direccion_dep" : registro['empleadoDetails']['detalles'][0]["direccionDepartamento"],
        "detalles_empleado_estado_civil" : registro['empleadoDetails']['detalles'][0]["estadoCivil"],
        "detalles_empleado_nivel_educacion" : registro['empleadoDetails']['detalles'][0]["nivelEducacional"],
        "detalles_empleado_profesion" : registro['empleadoDetails']['detalles'][0]["profesion"],
      
        "cargo" : registro["cargo"],        
        'fechaContratacion': registro["fechaContratacion"],
        'desde': registro["desde"],
        'hasta' : registro["hasta"],
        'finiquitado' : registro["finiquitado"],
        'unidadOrganizacional_id' : registro["unidadOrganizacional"],   
        #'sucursal' : registro["sucursal"]["nombre"],

        'jornada' : registro["jornada"]['id'], 
        'jornada_nombre' : registro["jornada"]['nombre'],  
        'horas_jornada' : registro["horasDeLaJornada"],
        'jefe' : registro["jefe"],
        "tipo_academico" : registro['userDefinedFields']['TipoDeAcademico'],
        "CategoriaAcademica" : registro['userDefinedFields']['CategoriaAcademica'],
        "titulo_academico" : registro['userDefinedFields']['NombreGradoTituloObtenido'],
        'Institucion_titulo' : registro['userDefinedFields']['InstitucionTitulo'],
        "centro_gestion" : registro['userDefinedFields']['CentroDeGestion'],
        "carrera_adscripcion" : registro['userDefinedFields']['CarreraDeAdscripcion'],
        "TipodePlantaAcademica" :  registro['userDefinedFields']['TipodePlantaAcademica'], 
       'tramoAsignacionPrevisional' : registro['tramoAsignacionPrevisional'],
       'isapre': registro['isapre'],
       'afp': registro['afp'],
       'sueldoBase': registro['sueldoBase'],
       'sueldoFormaPago': registro['sueldoFormaPago'],
       'sueldoBanco': registro['sueldoBanco'],
       'sueldoCuentaCorriente': registro['sueldoCuentaCorriente'],
       'sueldoCuentaCorrienteTipo': registro['sueldoCuentaCorrienteTipo'],
       'claseSalarial': registro['claseSalarial']


    }

mapeado1 = list(map(mi_funcion, results1))
df1 = pd.DataFrame(mapeado1)



result2 =[]
for i in range(0,len(results1)):
    if results1[i]['tipoContratoDetails'] != None:
        result2.append(results1[i])
        
        

# Ejemplo función, mapea 2 columnas de las solicitadas
def mi_funcion2(registro):
    return { 
         "id" : registro["id"],
        "tipo_contrato" :     registro['tipoContratoDetails']['nombre'],

       

    }


mapeado2 = list(map(mi_funcion2, result2))
df2 = pd.DataFrame(mapeado2)


result3 =[]
for i in range(0,len(results1)):
    if results1[i]['unidadOrganizacionalDetails'] != None:
        result3.append(results1[i])
        
        

# Ejemplo función, mapea 2 columnas de las solicitadas
def mi_funcion3(registro):
    return { 
         "id" : registro["id"],

        "UnidadOrganizacional":  registro['unidadOrganizacionalDetails']["nombre"]
       

    }


mapeado3 = list(map(mi_funcion3, result3))
df3 = pd.DataFrame(mapeado3)


result4 =[]
for i in range(0,len(results1)):
    if results1[i]['centroCosto'] != None:
        result4.append(results1[i])
        
        

# Ejemplo función, mapea 2 columnas de las solicitadas
def mi_funcion4(registro):
    return { 
         "id" : registro["id"],
        'centroCosto' : registro["centroCosto"]["nombre"],
        'centroCosto_id' : registro["centroCosto"]["id"],
        'centroCosto_codigo' : registro["centroCosto"]["codigo"]
    }
        


mapeado4 = list(map(mi_funcion4, result4))
df4 = pd.DataFrame(mapeado4)



df5 = pd.merge(df1, df2, on='id', how='outer')
df6 = pd.merge(df5, df3, on='id', how='outer')
df7 = pd.merge(df6, df4, on='id', how='outer')



quoted = urllib.parse.quote_plus("Driver= {SQL Server};Server=192.168.1.157;DataBase=DWHSource;UID=sa;PWD=S0portebi2@22")
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
df_talana = df7.to_sql("Talana_contratos_python_rec", schema='dbo', con=engine, if_exists="replace")
print("corrceto")

