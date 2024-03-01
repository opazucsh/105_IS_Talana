#!/usr/bin/env python
# coding: utf-8

# In[ ]:





import pandas as pd
import numpy as np
import sys
import requests
import json
import pyodbc
from sqlalchemy import create_engine
import urllib




url = "https://talana.com/es/api/remuneraciones/contract/all/?empresa=909&page_size=1000"

payload={}
headers = {
  'Authorization': 'Basic aW50ZWdyYWNpb25AdWNzaC5jbDpTb3BhaXBpbGxhSGV0ZXJvZG94YTg4',
  'Cookie': 'sessionid=o7y7gxbme43iwb9a2blmgo5tqtlt2v9p'
}

response = requests.request("GET", url, headers=headers, data=payload)

results   = response.text.encode('utf8')
results0  = json.loads(results)
results1  = results0["results"]



url = "https://talana.com/es/api/remuneraciones/contract/all/?empresa=909&page=2&page_size=1000"

payload={}
headers = {
  'Authorization': 'Basic aW50ZWdyYWNpb25AdWNzaC5jbDpTb3BhaXBpbGxhSGV0ZXJvZG94YTg4',
  'Cookie': 'sessionid=o7y7gxbme43iwb9a2blmgo5tqtlt2v9p'
}

response2 = requests.request("GET", url, headers=headers, data=payload)

results2   = response2.text.encode('utf8')
results20  = json.loads(results2)
results21  = results20["results"]    


     
url = "https://talana.com/es/api/remuneraciones/contract/all/?empresa=909&page=3&page_size=1000"

payload={}
headers = {
  'Authorization': 'Basic aW50ZWdyYWNpb25AdWNzaC5jbDpTb3BhaXBpbGxhSGV0ZXJvZG94YTg4',
  'Cookie': 'sessionid=o7y7gxbme43iwb9a2blmgo5tqtlt2v9p'
}

response3 = requests.request("GET", url, headers=headers, data=payload)

results3   = response3.text.encode('utf8')
results30  = json.loads(results3)
results31  = results30["results"]   
    


url = "https://talana.com/es/api/remuneraciones/contract/all/?empresa=909&page=4&page_size=1000"

payload={}
headers = {
  'Authorization': 'Basic aW50ZWdyYWNpb25AdWNzaC5jbDpTb3BhaXBpbGxhSGV0ZXJvZG94YTg4',
  'Cookie': 'sessionid=o7y7gxbme43iwb9a2blmgo5tqtlt2v9p'
}

response4 = requests.request("GET", url, headers=headers, data=payload)

results4   = response4.text.encode('utf8')
results40  = json.loads(results4)
results41  = results40["results"]

url = "https://talana.com/es/api/remuneraciones/contract/all/?empresa=909&page=5&page_size=1000"

payload={}
headers = {
  'Authorization': 'Basic aW50ZWdyYWNpb25AdWNzaC5jbDpTb3BhaXBpbGxhSGV0ZXJvZG94YTg4',
  'Cookie': 'sessionid=o7y7gxbme43iwb9a2blmgo5tqtlt2v9p'
}

response5 = requests.request("GET", url, headers=headers, data=payload)

results5   = response5.text.encode('utf8')
results50  = json.loads(results5)
results51  = results50["results"]


url = "https://talana.com/es/api/remuneraciones/contract/all/?empresa=909&page=6&page_size=1000"

payload={}
headers = {
  'Authorization': 'Basic aW50ZWdyYWNpb25AdWNzaC5jbDpTb3BhaXBpbGxhSGV0ZXJvZG94YTg4',
  'Cookie': 'sessionid=o7y7gxbme43iwb9a2blmgo5tqtlt2v9p'
}

response6 = requests.request("GET", url, headers=headers, data=payload)

results6   = response6.text.encode('utf8')
results60  = json.loads(results6)
results61  = results60["results"]

results22 = results1+results21+results31+results41+results51+results61



def mi_funcion(registro):
    return {    "id" : registro["id"],

        "empleado_id" : registro['empleado']["id"],
        "empleado_rut" : registro['empleado']["rut"],
        "empleado_nombre" : registro['empleado']["nombre"],
        "empleado_paterno" : registro['empleado']["apellidoPaterno"],
        "empleado_materno" : registro['empleado']["apellidoMaterno"],
        "empleado_sexo" : registro['empleado']["sexo"],
        "empleado_fecha_nacimiento" : registro['empleado']["fechaNacimiento"],
        "emplead_nacionalidad" : registro['empleado']["nacionalidad"],
        "empleado_email" : registro['empleado']["email"]  ,       
       
        "detalles_empleado_id" : registro['empleado']['detalles'][0]["id"],
        "detalles_empleado_foto" : registro['empleado']['detalles'][0]["foto"], 
            
        "detalles_empleado_mail_personal" : registro['empleado']['detalles'][0]["emailPersonal"],
        "detalles_empleado_telefono" : registro['empleado']['detalles'][0]["telefono"],
        "detalles_empleado_celular" : registro['empleado']['detalles'][0]["celular"],
        "detalles_empleado_direccioncalle" : registro['empleado']['detalles'][0]["direccionCalle"],
        "detalles_empleado_direccionnumero" : registro['empleado']['detalles'][0]["direccionNumero"],
        "detalles_empleado_direccion_dep" : registro['empleado']['detalles'][0]["direccionDepartamento"],
        "detalles_empleado_estado_civil" : registro['empleado']['detalles'][0]["estadoCivil"],
        "detalles_empleado_nivel_educacion" : registro['empleado']['detalles'][0]["nivelEducacional"],
        "detalles_empleado_profesion" : registro['empleado']['detalles'][0]["profesion"],    
        
        "cargo" : registro["cargo"],        
        'fechaContratacion': registro["fechaContratacion"],
        'desde': registro["desde"],
        'hasta' : registro["hasta"],
        'finiquitado' : registro["finiquitado"],
            
            
              

    
            

       # 'unidadOrganizacional_id' : registro["unidadOrganizacional"],
       # 'sucursal' : registro["sucursal"],
       # 'centroCosto' : registro["centroCosto"]['nombre'],
        'jornada' : registro["jornada"]['id'], 
        'jornada_nombre' : registro["jornada"]['nombre'],  
        'horas_jornada' : registro["horasDeLaJornada"],
            
        "CategoriaAcademica" : registro['userDefinedFields']['CategoriaAcademica'],
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

mapeado = list(map(mi_funcion,results22 ))
df = pd.DataFrame(mapeado)



def mi_funcion2(registro):
    return {    "id" : registro["id"],      
        "tipo_contrato" :     registro['tipoContratoDetails']['nombre']    


    }


result2 =[]
for i in range(0,len(results22)):
    if results22[i]['tipoContratoDetails'] != None:
        result2.append(results22[i]) 
        
        
mapeado2 = list(map(mi_funcion2, result2))
df2 = pd.DataFrame(mapeado2) 


def mi_funcion3(registro):
    return {    "id" : registro["id"],      

         "UnidadOrganizacional":  registro['unidadOrganizacional']["nombre"]
    }


result3 =[]
for i in range(0,len(results22)):
    if results22[i]['unidadOrganizacional'] != None:
        result3.append(results22[i]) 
        
        
mapeado3 = list(map(mi_funcion3, result3))
df3 = pd.DataFrame(mapeado3) 

def mi_funcion4(registro):
    return {    "id" : registro["id"],      
         'centroCosto' : registro["centroCosto"]['nombre'],      
         'centroCosto_id' : registro["centroCosto"]['id'],    
         'centroCosto_codigo' : registro["centroCosto"]['codigo'] 

    }


result4 =[]
for i in range(0,len(results22)):
    if results22[i]['centroCosto'] != None:
        result4.append(results22[i]) 
        
        
mapeado4 = list(map(mi_funcion4, result4))
df4 = pd.DataFrame(mapeado4) 


def mi_funcion5(registro):
    return {       
          "id" : registro["id"],
         'jefe_id' : registro["jefe"]['id'],
         'jefe_rut' : registro["jefe"]['rut'],
         'jefe_email' : registro["jefe"]['email'],

    }


result5 =[]
for i in range(0,len(results22)):
    if results22[i]['jefe'] != None:
        result5.append(results22[i]) 
        
        
mapeado5 = list(map(mi_funcion5, result5))
df5 = pd.DataFrame(mapeado5)  

df6 = pd.merge(df, df2, on='id', how='outer')
df7 = pd.merge(df6, df3, on='id', how='outer')

df8 = pd.merge(df7, df4, on='id', how='outer')
df9 = pd.merge(df8, df5, on='id', how='outer')



quoted = urllib.parse.quote_plus("Driver= {SQL Server};Server=192.168.1.157;DataBase=DWHSource;UID=sa;PWD=S0portebi2@22")
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
df_talana2 = df9.to_sql("Talana_remuneraciones_h_python_rec", schema='dbo', con=engine, if_exists="replace")
print("corrceto")