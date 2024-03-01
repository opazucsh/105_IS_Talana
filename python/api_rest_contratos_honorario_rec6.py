#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!/usr/bin/env python
# coding: utf-8

# In[13]:


#!/usr/bin/env python
# coding: utf-8

# In[32]:



import pandas as pd
import numpy as np
import sys
import requests
import json
import pyodbc
from sqlalchemy import create_engine
import urllib



url = "https://talana.com/es/api/contrato/?empresa=909"


headers = {
  'Authorization': 'Basic aW50ZWdyYWNpb25AdWNzaC5jbDpTb3BhaXBpbGxhSGV0ZXJvZG94YTg4',
  'Cookie': 'sessionid=mingt94ry1vzl3sbcis8zn4g0nwekmie'
}

response = requests.request("GET", url, headers=headers, data =  {})

results  = response.text.encode('utf8')
results = json.loads(results)

        
        
    



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


mapeado = list(map(mi_funcion, results))
df = pd.DataFrame(mapeado)




def mi_funcion2(registro):
    return {    "id" : registro["id"],      
        "tipo_contrato" :     registro['tipoContratoDetails']['nombre']    


    }


result2 =[]
for i in range(0,len(results)):
    if results[i]['tipoContratoDetails'] != None:
        result2.append(results[i]) 
        
        
mapeado2 = list(map(mi_funcion2, result2))
df2 = pd.DataFrame(mapeado2) 


def mi_funcion3(registro):
    return {    "id" : registro["id"],      
         'unidadOrganizacional' : registro["unidadOrganizacionalDetails"]['nombre']  


    }


result3 =[]
for i in range(0,len(results)):
    if results[i]['unidadOrganizacionalDetails'] != None:
        result3.append(results[i]) 
        
        
mapeado3 = list(map(mi_funcion3, result3))
df3 = pd.DataFrame(mapeado3) 



def mi_funcion4(registro):
    return {    "id" : registro["id"],      
         'centroCosto' : registro["centroCosto"]['nombre'],      
         'centroCosto_id' : registro["centroCosto"]['id'],    
         'centroCosto_codigo' : registro["centroCosto"]['codigo'] 

    }


result4 =[]
for i in range(0,len(results)):
    if results[i]['centroCosto'] != None:
        result4.append(results[i]) 
        
        
mapeado4 = list(map(mi_funcion4, result4))
df4 = pd.DataFrame(mapeado4)  


def mi_funcion5(registro):
    return {    "id" : registro["id"],      
         "jefe" : registro["jefe"]    

    }


result5 =[]
for i in range(0,len(results)):
    if results[i]['jefe'] != None:
        result5.append(results[i]) 
        
        
mapeado5 = list(map(mi_funcion5, result5))
df5 = pd.DataFrame(mapeado5)  




df6 = pd.merge(df, df2, on='id', how='outer')
df7 = pd.merge(df6, df3, on='id', how='outer')

df8 = pd.merge(df7, df4, on='id', how='outer')
df9 = pd.merge(df8, df5, on='id', how='outer')




df_muestra = df9.loc[:,
[
    'id',
    

        "empleado"  ,
        "empleado_id" ,
        "empleado_rut" ,
        "empleado_nombre" ,
        "empleado_paterno" ,
        "empleado_materno" ,
        "empleado_sexo" ,
        "empleado_fecha_nacimiento" ,
        "emplead_nacionalidad",
        "empleado_email"   ,       
        "detalles_empleado_id" ,
        "detalles_empleado_mail_personal" ,
        "detalles_empleado_telefono" ,
        "detalles_empleado_celular" ,
        "detalles_empleado_direccioncalle" ,
        "detalles_empleado_direccionnumero" ,
        "detalles_empleado_direccion_dep" ,
        "detalles_empleado_estado_civil" ,
        "detalles_empleado_nivel_educacion" ,
        "detalles_empleado_profesion" ,
        "tipo_contrato" ,
        "unidadOrganizacional_id",
        "unidadOrganizacional",
        "centroCosto",
        'centroCosto_id',    
        'centroCosto_codigo',
    
    
        "cargo" ,        
        'fechaContratacion',
        'desde',
        'hasta' ,
        'finiquitado' ,

        'jornada' ,
        'horas_jornada' ,

       'tramoAsignacionPrevisional' ,
       'isapre',
       'afp',
       'sueldoBase',
       'sueldoFormaPago',
       'sueldoBanco',
       'sueldoCuentaCorriente',
       'sueldoCuentaCorrienteTipo',
       'claseSalarial',
       'jefe',
        "centro_gestion" ,
        "carrera_adscripcion" ,
        "TipodePlantaAcademica",
        "CategoriaAcademica"
    
    
]   ]

df_muestra.drop_duplicates(subset="empleado_id",  keep='first', inplace=True, ignore_index=True)


quoted = urllib.parse.quote_plus("Driver= {SQL Server};Server=192.168.1.157;DataBase=DWHSource;UID=sa;PWD=S0portebi2@22")
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
df_talana2 = df_muestra.to_sql("Talana_contratos_h_python", schema='dbo', con=engine, if_exists="replace")


# In[ ]:


# In[ ]:




