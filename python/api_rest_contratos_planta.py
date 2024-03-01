import pandas as pd
import numpy as np
import sys
import requests
import json
import pyodbc
from sqlalchemy import create_engine
import urllib




url = "https://talana.com/es/api/contrato/"

#payload = {}
headers = {  'Authorization': 'Basic aW50ZWdyYWNpb25AdWNzaC5jbDpTb3BhaXBpbGxhSGV0ZXJvZG94YTg4',
  'Cookie': 'sessionid=qgm8r88pr6j1qq1zsm6ijrw5nb430eay'}

response = requests.request("GET", url, headers=headers, data =  {})

results0  = response.text.encode('utf8')
results1 = json.loads(results0)


# Ejemplo función, mapea 2 columnas de las solicitadas
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
        "tipo_contrato" :     registro['tipoContratoDetails']['nombre'],
        "cargo" : registro["cargo"],        
        'fechaContratacion': registro["fechaContratacion"],
        'desde': registro["desde"],
        'hasta' : registro["hasta"],
        'finiquitado' : registro["finiquitado"],
        'unidadOrganizacionalDetails' : registro["unidadOrganizacionalDetails"]["nombre"],
        #'sucursal' : registro["sucursal"]["nombre"],
        'centroCosto' : registro["centroCosto"]["nombre"],
        'jornada' : registro["jornada"],
        'horas_jornada' : registro["horasDeLaJornada"],
        "UnidadOrganizacional":  registro['unidadOrganizacionalDetails']["nombre"],
        "tipo_academico" : registro['userDefinedFields']['TipoDeAcademico'],
        "titulo_academico" : registro['userDefinedFields']['NombreGradoTituloObtenido'],
        'Institucion_titulo' : registro['userDefinedFields']['InstitucionTitulo'],
        "centro_gestion" : registro['userDefinedFields']['CentroDeGestion'],
        "carrera_adscripcion" : registro['userDefinedFields']['CarreraDeAdscripcion'],
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


mapeado = list(map(mi_funcion, results1))
df = pd.DataFrame(mapeado)




quoted = urllib.parse.quote_plus("Driver= {SQL Server};Server=192.168.1.105;DataBase=DWHSource;UID=sa;PWD=Hellyonor41")
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
df_talana = df.to_sql("Talana_contratos_python", schema='dbo', con=engine, if_exists="replace")
print("corrceto")