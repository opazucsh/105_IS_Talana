#!/usr/bin/env python
# coding: utf-8

# In[18]:


import pandas as pd
import numpy as np
import sys
import requests
import json
import pyodbc
from sqlalchemy import create_engine
import urllib



url = "https://talana.com/es/api/contrato/?empresa=909"

payload={}
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
        "idContrato" : registro["idContrato"],
        "empresa" : registro["empresa"] ,
        "empleado" : registro["empleado"] ,
        
        "empleadoDetails_id" :     registro['empleadoDetails']['id'],
        "empleadoDetail_fechaCreacion" : registro['empleadoDetails']['fechaCreacion'],
        "empleadoDetail_rut" : registro['empleadoDetails']['rut'],
        "empleadoDetails_nombre" :     registro['empleadoDetails']['nombre'],
        "empleadoDetails_apellidoPaterno" :     registro['empleadoDetails']['apellidoPaterno'],
        "empleadoDetails_apellidoMaterno" :     registro['empleadoDetails']['apellidoMaterno'],
        "empleadoDetails_sexo" :     registro['empleadoDetails']['sexo'],
        "empleadoDetails_fechaNacimiento" :     registro['empleadoDetails']['fechaNacimiento'],
        "empleadoDetails_nacionalidad" :     registro['empleadoDetails']['nacionalidad'],
        "empleadoDetails_username" :     registro['empleadoDetails']['username'],
        "empleadoDetails_email" :     registro['empleadoDetails']['email'],
        "empleadoDetails_externalReference" :     registro['empleadoDetails']['externalReference'],

        
        
        "codigo" : registro["codigo"],
        "tipoContrato" : registro["tipoContrato"] ,

        "empleadorRazonSocial" : registro['empleadorRazonSocial'],
        "fechaCreacion" : registro['fechaCreacion'],
        "fechaModificacion" : registro['fechaModificacion'],     
        "cargo" : registro["cargo"],        
        'fechaContratacion': registro["fechaContratacion"],
        'desde': registro["desde"],
        'hasta' : registro["hasta"],
        'finiquitado' : registro["finiquitado"],
        
        'unidadOrganizacional': registro["unidadOrganizacional"],
        #'unidadOrganizacionalDetails': registro["unidadOrganizacionalDetails"]["nombre"],


        #"motivoEgreso" : registro['motivoEgreso'],
        "anexo" : registro['anexo'],     
       
       #"centro_costo" : registro['centroCosto'],
       # 'centroCosto_id' : registro["centroCosto"]["id"],
       # 'centroCosto_parent' : registro["centroCosto"]["parent"],
       # 'centroCosto_empresa' : registro["centroCosto"]["empresa"],
       # 'centroCosto_codigo' : registro["centroCosto"]["codigo"],
       #'centroCosto_nombre' : registro["centroCosto"]["nombre"],
       # 'centroCosto_externalReference' : registro["centroCosto"]["externalReference"],
        
        'jornada' : registro["jornada"]['id'], 
        'jornada_nombre' : registro["jornada"]['nombre'],  
        'horas_jornada' : registro["horasDeLaJornada"],
        'codigoFranquiciaSence' : registro["codigoFranquiciaSence"],
        'nivelSence' : registro["nivelSence"],
        'ine' : registro["INE"],
        'sindicato' : registro["sindicato"],
        'jefe' : registro["jefe"],
        'esPensionado' : registro["esPensionado"],
        'tramoAsignacionPrevisional' : registro["tramoAsignacionPrevisional"],
        'zonaAsignacionPrevisional' : registro["zonaAsignacionPrevisional"],
        'correspondeAsignacionMaternal' : registro["correspondeAsignacionMaternal"],
        'isapre' : registro["isapre"],
        'montoPactadoIsapre' : registro["montoPactadoIsapre"],
        'montoPactadoIsapreMoneda' : registro["montoPactadoIsapreMoneda"],
        'montoPactado2Isapre' : registro["montoPactado2Isapre"],
        'afp' : registro["afp"],
        'sueldoBase' : registro["sueldoBase"],
        'sueldoFormaPago' : registro["sueldoFormaPago"],
        'sueldoBanco' : registro["sueldoBanco"],
        'sueldoCuentaCorriente' : registro["sueldoCuentaCorriente"],
        
        'sueldoCuentaCorrienteTipo' : registro["sueldoCuentaCorrienteTipo"],
        'sueldoTipoPago' : registro["sueldoTipoPago"],
        'valorHoraExtraPactada' : registro["valorHoraExtraPactada"],
        'mesesImponiblesreconocidos' : registro["mesesImponiblesreconocidos"],
        'mesesImponiblesReconocidosDesde' : registro["mesesImponiblesReconocidosDesde"],
        'vacacionesReconocidoDesde' : registro["vacacionesReconocidoDesde"],
        'asignacionMovilizacion' : registro["asignacionMovilizacion"],
        'asignacionColacion' : registro["asignacionColacion"],
        'anticipoPactado' : registro["anticipoPactado"],
        'fechaDeContratacionReconocidaParaAnosDeServicio' : registro["fechaDeContratacionReconocidaParaAnosDeServicio"],
        'pagaTresPrimerosDiasLicencia' : registro["pagaTresPrimerosDiasLicencia"],
        'mantieneRentaLiquidaLicencia' : registro["mantieneRentaLiquidaLicencia"],
        'diasAdministrativos' : registro["diasAdministrativos"],
        'indemnizacionSinTopeAnos' : registro["indemnizacionSinTopeAnos"],
        'indemnizacionSinTopeRenta' : registro["indemnizacionSinTopeRenta"],
        'diasAdicionalesVacaciones' : registro["diasAdicionalesVacaciones"],
        
        
        
        'descripcionDelCargo' : registro["descripcionDelCargo"],
        'clausulasAdicionales' : registro["clausulasAdicionales"],
        'detalleAnexoContrato' : registro["detalleAnexoContrato"],
        'documentoEsContratoOAnexo' : registro["documentoEsContratoOAnexo"],
        
        'claseSalarial' : registro["claseSalarial"],
        'rolPrivado' : registro["rolPrivado"],
        'userDefinedField_grado' : registro["userDefinedFields"]["Grado"],  
        'userDefinedField_tipoRemuneracion' : registro["userDefinedFields"]["Tipo de Remuneración"],  
        'userDefinedField_estamento' : registro["userDefinedFields"]["Estamento"],  
        'userDefinedField_clasificacion' : registro["userDefinedFields"]["Clasificacion"],  
        'externalReference' : registro["externalReference"],
        'asignacionZonaExtrema' : registro["asignacionZonaExtrema"],
        
        'adscribeASeguroCesantiaParaContratosPreviosA2002' : registro["adscribeASeguroCesantiaParaContratosPreviosA2002"],
        
        'sueldoPatronal' : registro["sueldoPatronal"],
        'depositoConvenidoMonto' : registro["depositoConvenidoMonto"],
        'depositoConvenidoMoneda' : registro["depositoConvenidoMoneda"],
        'retencionJuducialDestinatario' : registro["retencionJuducialDestinatario"],
                
        'apvMonto' : registro["apvMonto"],
        'apvMoneda' : registro["apvMoneda"],
        'apvInstitucion' : registro["apvInstitucion"],
        'apvTipo' : registro["apvTipo"],
        'apvCuentaDos' : registro["apvTipo"],
        'apvCuentaDosMoneda' : registro["apvTipo"],
        'beneficiosInfoAdicional' : registro["beneficiosInfoAdicional"],
        'creadoPor' : registro["creadoPor"]

       


        
    }

mapeado = list(map(mi_funcion, results1))
df = pd.DataFrame(mapeado)



def mi_funcion2(registro):
    return {    
        "id" : registro["id"],
        "tipo_contratoDetails_Id" :     registro['tipoContratoDetails']['id'],
        "tipo_contratoDetails_Nombre" :     registro['tipoContratoDetails']['nombre']
           }
    
    
result2 =[]
for i in range(0,len(results1)):
    if results1[i]['tipoContratoDetails'] != None:
        result2.append(results1[i])   
        
        
mapeado2 = list(map(mi_funcion2, result2))
df2 = pd.DataFrame(mapeado2)     

df3 = pd.merge(df, df2, on='id', how='outer')


df_muestra = df3.loc[:,
[
    'id',

        "idContrato" ,
        "empresa" ,
        "empleado"  ,
        
        "empleadoDetails_id" ,
        "empleadoDetail_fechaCreacion" ,
        "empleadoDetail_rut" ,
        "empleadoDetails_nombre" ,
        "empleadoDetails_apellidoPaterno",
        "empleadoDetails_apellidoMaterno" ,
        "empleadoDetails_sexo" ,
        "empleadoDetails_fechaNacimiento" ,
        "empleadoDetails_nacionalidad" ,
        "empleadoDetails_username" ,
        "empleadoDetails_email" ,
        "empleadoDetails_externalReference" ,

        
        
        "codigo" ,
        "tipoContrato" ,
        "tipo_contratoDetails_Id",
        "tipo_contratoDetails_Nombre" ,
        "empleadorRazonSocial" ,
        "fechaCreacion" ,
        "fechaModificacion" ,     
        "cargo",        
        'fechaContratacion',
        'desde',
        'hasta' ,
        'finiquitado' ,
        
        'unidadOrganizacional',

        "anexo" ,     
       

        
        'jornada' ,
        'horas_jornada' ,
        'codigoFranquiciaSence',
        'nivelSence' ,
        'ine' ,
        'sindicato' ,
        'jefe' ,
        'esPensionado',
        'tramoAsignacionPrevisional' ,
        'zonaAsignacionPrevisional' ,
        'correspondeAsignacionMaternal' ,
        'isapre' ,
        'montoPactadoIsapre' ,
        'montoPactadoIsapreMoneda' ,
        'montoPactado2Isapre' ,
        'afp' ,
        'sueldoBase' ,
        'sueldoFormaPago' ,
        'sueldoBanco' ,
        'sueldoCuentaCorriente' ,
        
        'sueldoCuentaCorrienteTipo' ,
        'sueldoTipoPago' ,
        'valorHoraExtraPactada' ,
        'mesesImponiblesreconocidos' ,
        'mesesImponiblesReconocidosDesde' ,
        'vacacionesReconocidoDesde' ,
        'asignacionMovilizacion' ,
        'asignacionColacion' ,
        'anticipoPactado' ,
        'fechaDeContratacionReconocidaParaAnosDeServicio' ,
        'pagaTresPrimerosDiasLicencia' ,
        'mantieneRentaLiquidaLicencia' ,
        'diasAdministrativos' ,
        'indemnizacionSinTopeAnos' ,
        'indemnizacionSinTopeRenta' ,
        'diasAdicionalesVacaciones' ,
        
        
        
        'descripcionDelCargo' ,
        'clausulasAdicionales' ,
        'detalleAnexoContrato' ,
        'documentoEsContratoOAnexo' ,
        
        'claseSalarial' ,
        'rolPrivado' ,
        'userDefinedField_grado' ,  
        'userDefinedField_tipoRemuneracion' ,  
        'userDefinedField_estamento' ,  
        'userDefinedField_clasificacion' ,  
        'externalReference' ,
        'asignacionZonaExtrema' ,
        
        'adscribeASeguroCesantiaParaContratosPreviosA2002' ,
        
        'sueldoPatronal' ,
        'depositoConvenidoMonto' ,
        'depositoConvenidoMoneda' ,
        'retencionJuducialDestinatario' ,
                
        'apvMonto' ,
        'apvMoneda' ,
        'apvInstitucion' ,
        'apvTipo' ,
        'apvCuentaDos' ,
        'apvCuentaDosMoneda' ,
        'beneficiosInfoAdicional' ,
        'creadoPor' 

]   ]

quoted = urllib.parse.quote_plus("Driver= {SQL Server};Server=192.168.1.157;DataBase=DWHSource;UID=sa;PWD=S0portebi2@22")
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
df_talana = df_muestra.to_sql("Talana_contratos_python_extendido", schema='dbo', con=engine, if_exists="replace")
print("corrceto")