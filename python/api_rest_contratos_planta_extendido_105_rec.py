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




url = "https://talana.com/es/api/contrato/"

#payload = {}
headers = {  'Authorization': 'Basic aW50ZWdyYWNpb25AdWNzaC5jbDpTb3BhaXBpbGxhSGV0ZXJvZG94YTg4',
  'Cookie': 'sessionid=qgm8r88pr6j1qq1zsm6ijrw5nb430eay'}

response = requests.request("GET", url, headers=headers, data =  {})

results0  = response.text.encode('utf8')
results1 = json.loads(results0)




# Ejemplo función, mapea 2 columnas de las solicitadas
def mi_funcion(registro):
    return {    
        "id" : registro["id"],
        "idContrato" : registro["idContrato"],
        "empresa" : registro["empresa"] ,
        "empleado" : registro["empleado"] ,
        "codigo" : registro["codigo"] ,
        "tipoContrato" : registro["tipoContrato"] ,
    
        "empleadorRazonSocial" : registro['empleadorRazonSocial'],
        "fechaCreacion" : registro['fechaCreacion'],
        "fechaModificacion" : registro['fechaModificacion'],     
        "cargo" : registro["cargo"],        
        'fechaContratacion': registro["fechaContratacion"],
        'desde': registro["desde"],
        'hasta' : registro["hasta"],
        'finiquitado' : registro["finiquitado"],
        
  
   

        
        
        "grupos" : registro['grupos'],        
        "motivoEgreso" : registro['motivoEgreso'],
        "anexo" : registro['anexo'],     
        

        
        
        'jornada' : registro["jornada"]['id'], 
        'jornada_nombre' : registro["jornada"]['nombre'],  
        'horas_jornada' : registro["horasDeLaJornada"],
        'codigoFranquiciaSence' : registro["codigoFranquiciaSence"],
        'nivelSence' : registro["nivelSence"],

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
        
        'secretarias' : registro["userDefinedFields"]["Secretarias"],
        'ocupacion' : registro["userDefinedFields"]["Ocupacion"],        
        'fechaEnQueObtuvo' : registro["userDefinedFields"]["FechaEnQueObtuvo"],
        'asignacionMatrimonio' : registro["userDefinedFields"]["asignacionMatrimonio"],
        'categoriaContable' : registro["userDefinedFields"]["categoriaContable"],        
        'estamento' : registro["userDefinedFields"]["Estamento"],        


        
        
        "tipo_academico" : registro['userDefinedFields']['TipoDeAcademico'],
        "titulo_academico" : registro['userDefinedFields']['NombreGradoTituloObtenido'],
        'Institucion_titulo' : registro['userDefinedFields']['InstitucionTitulo'],
        "centro_gestion" : registro['userDefinedFields']['CentroDeGestion'],
        "carrera_adscripcion" : registro['userDefinedFields']['CarreraDeAdscripcion'],
        
        
        
        'telefonoParticular' : registro["userDefinedFields"]["TelefonoParticular"],

        'paisDondeObtuvo' : registro["userDefinedFields"]["PaisDondeObtuvo"],
        
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
        "tipo_contratoDetails_Id" :     registro['tipoContratoDetails']['id'],
        "tipo_contratoDetails_Nombre" :     registro['tipoContratoDetails']['nombre'],
        'unidadOrganizacionalDetails_parent' : registro["unidadOrganizacionalDetails"]["parent"],
        'unidadOrganizacionalDetails_nombre' : registro["unidadOrganizacionalDetails"]["nombre"],
        'unidadOrganizacionalDetails_externalReference' : registro["unidadOrganizacionalDetails"]["externalReference"],
        'unidadOrganizacionalDetails_id' : registro["unidadOrganizacionalDetails"]["id"],
        'unidadOrganizacionalDetails_codigo' : registro["unidadOrganizacionalDetails"]["codigo"],
        'centroCosto_id' : registro["centroCosto"]["id"],
        'centroCosto_parent' : registro["centroCosto"]["parent"],
        'centroCosto_empresa' : registro["centroCosto"]["empresa"],
        'centroCosto_codigo' : registro["centroCosto"]["codigo"],
        'centroCosto_nombre' : registro["centroCosto"]["nombre"],
        'centroCosto_externalReference' : registro["centroCosto"]["externalReference"],
         'ineNombre' : registro["INE"]["nombre"],
        'ineCodigo' : registro["INE"]["codigo"]


       

    }
     
mapeado2 = list(map(mi_funcion2, result2))

df2 = pd.DataFrame(mapeado2)

df3 = pd.merge(df1, df2, on='id', how='outer')


df_muestra = df3.loc[:,
[
    'id',
    'idContrato',
    'empresa',
    'empleado',
    'codigo',
    'tipoContrato', 
    'tipo_contratoDetails_Id',
    'tipo_contratoDetails_Nombre',
    'empleadorRazonSocial',
    'fechaCreacion',
    'fechaModificacion',
    'cargo',
    'fechaContratacion',
    'desde',
    'hasta',
    'finiquitado',

    'anexo',
    'centroCosto_id',
    'centroCosto_parent',
    'centroCosto_empresa',
    'centroCosto_codigo',
    'centroCosto_nombre',
    'centroCosto_externalReference',
    'jornada',
    'horas_jornada',
    'codigoFranquiciaSence',
    'nivelSence',
    'ineNombre',
    'ineCodigo',
    'sindicato',
    'jefe',
    'esPensionado',
    'tramoAsignacionPrevisional',
    'zonaAsignacionPrevisional',
    'correspondeAsignacionMaternal',
    'isapre',
    'montoPactadoIsapre',
    'montoPactadoIsapreMoneda',
    'montoPactado2Isapre',
    'afp',
    'sueldoBase',
    'sueldoFormaPago',
    'sueldoBanco',
    'sueldoCuentaCorriente',
    'sueldoCuentaCorrienteTipo',
    'sueldoTipoPago',
    'valorHoraExtraPactada',
    'mesesImponiblesreconocidos',
    'mesesImponiblesReconocidosDesde',
    'vacacionesReconocidoDesde',
    'asignacionMovilizacion',
    'asignacionColacion',
    'anticipoPactado',
    'fechaDeContratacionReconocidaParaAnosDeServicio',
    'pagaTresPrimerosDiasLicencia',
    'mantieneRentaLiquidaLicencia',
    'diasAdministrativos', 
    'indemnizacionSinTopeAnos', 
    'indemnizacionSinTopeRenta',
    'diasAdicionalesVacaciones',
    'descripcionDelCargo',
    'clausulasAdicionales',
    'detalleAnexoContrato',
    'documentoEsContratoOAnexo', 
    'claseSalarial', 
    'rolPrivado', 
    'secretarias', 
    'ocupacion', 
    'fechaEnQueObtuvo', 
    'asignacionMatrimonio', 
    'categoriaContable', 
    'estamento', 
    'tipo_academico', 
    'titulo_academico', 
    'Institucion_titulo',
    'centro_gestion', 
    'carrera_adscripcion', 
    'telefonoParticular', 
    'paisDondeObtuvo', 
    'externalReference', 
    'asignacionZonaExtrema', 
    'adscribeASeguroCesantiaParaContratosPreviosA2002', 
    'sueldoPatronal', 
    'depositoConvenidoMonto', 
    'depositoConvenidoMoneda', 
    'retencionJuducialDestinatario', 
    'apvMonto',
    'apvMoneda', 
    'apvInstitucion', 
    'apvTipo',
    'apvCuentaDos', 
    'apvCuentaDosMoneda', 
    'beneficiosInfoAdicional',
    'creadoPor',
    'unidadOrganizacionalDetails_parent',
    'unidadOrganizacionalDetails_nombre',
    'unidadOrganizacionalDetails_externalReference',
    'unidadOrganizacionalDetails_id',
    'unidadOrganizacionalDetails_codigo'

 

]   ]

quoted = urllib.parse.quote_plus("Driver= {SQL Server};Server=192.168.1.157;DataBase=DWHSource;UID=sa;PWD=S0portebi2@22")
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
df_talana = df_muestra.to_sql("Talana_contratos_extendido", schema='dbo', con=engine, if_exists="replace")
print("corrceto")


