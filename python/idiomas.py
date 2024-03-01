#!/usr/bin/env python
# coding: utf-8

# In[1]:


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



url = "https://talana.com/es/api/idioma/"


headers = {
'Authorization': 'Basic aW50ZWdyYWNpb25AdWNzaC5jbDpTb3BhaXBpbGxhSGV0ZXJvZG94YTg4'
}

response = requests.request("GET", url, headers=headers, data =  {})

results0  = response.text.encode('utf8')
results1 = json.loads(results0)




# Ejemplo función, mapea 2 columnas de las solicitadas
def mi_funcion(registro):
    return {  
        
        
        
        
        "id" : registro["id"],
        "nombre" : registro["nombre"]

        
        
        
        

        

    }



mapeado = list(map(mi_funcion, results1))
df = pd.DataFrame(mapeado)


quoted = urllib.parse.quote_plus("Driver= {SQL Server};Server=192.168.1.157;DataBase=DWHSource;UID=sa;PWD=S0portebi2@22")
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
df_talana = df.to_sql("Talana_Idioma_paso", schema='dbo', con=engine, if_exists="replace")
print("correcto")