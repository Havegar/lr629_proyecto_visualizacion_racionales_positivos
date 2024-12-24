#!/usr/bin/env python
# coding: utf-8

# ## Apiux & SII: Propagacion de contaminacion de contribuyentes en la malla comercial.
# 
# ## Henry Vega (henrry.vega@api-ux.com)
# ## Data scientist

# Para poder realizar una propagacion de riesgo, consideraremos que éste se puede propagar debido a una influencia directa de un contribuyente sobre otro. Por ejemplo, al considerar una relación patrimonial o familiar, o debido a un impacto indirecto, como al considerar una relación comercial donde por ejemplo si una de las partes en la relación no cumple de forma correcta todas sus obligaciones tributarias podría afectar a que la otra parte no cumpla o cumpla parcialmente con las suyas, luego en este caso no hay un influencia directa en el comportamiento del contribuyente contaminado, sino que la contaminación se da de forma indirecta. En el caso de este algoritmo propuesto, se consideran las relaciones comerciales, considerando el IVA de las transacciones de una entidad con otra. Junto con las fuerzas de relaciones, hay un valor de contaminacion que inicialmente es 0 o 1 para cada entidad (considerando que ya estaba contaminado en base a alguna alerta o no lo esta). La propagacion se ejecuta con estas condiciones iniciales.
# 
# ![image.png](attachment:7c1b6e30-5da8-4480-bc66-f1fe06410251.png)

# La fuerza entre entidades tiene la condicion de creacion de estar en el rango [0,1].
# 
# ![image.png](attachment:2223f3b1-b3ef-4752-8029-c699f9be904b.png)

# Eventualmente, muchas entidades A se pueden relacionar comercialmente con B, segn el siguiente diagrama. De esta forma se tendira que actualizar el valor de scoring de B de acuerdo a los valores de scoring de A y las fuerzas correspondientes.
# Como ejemplo, consideremos tres entidades A que se han relacionado comercialmente con una entidad B. 
# 
# ![image.png](attachment:23ac9bef-ee03-4fd0-a73a-b3f6a89ff31a.png)

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
import pyspark
import pandas as pd
import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)

from pyspark_dist_explore import hist
import matplotlib.pyplot as plt


# In[ ]:


ss_name = 'Ejecucion algoritmo propagación de contaminación'
wg_conn = "spark.kerberos.access.hadoopFileSystems"
db_conn = "abfs://data@datalakesii.dfs.core.windows.net/"

spark = SparkSession.builder \
      .appName(f"Ejecucion algoritmo {ss_name}")  \
      .config(wg_conn, db_conn) \
      .config("spark.executor.memory", "6g") \
      .config("spark.driver.memory", "12g")\
      .config("spark.executor.cores", "4") \
      .config("spark.executor.instances", "5") \
      .config("spark.driver.maxResultSize", "12g") \
      .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")


# El algoritmo se basa en establecer la fuerza de las relaciones comerciales entre entidades y luego realizar una actualizacion de los valores de contaminacion para cada uno de ellos.La propagacion de contaminacion se hara hacia adelante, es decir lo receptores de factura son contaminados por las entidades emisoras. 

# Leemos la data de fuerza a partir de los datos procesados, de una fuerza para cada arco entre contribuyente A y B.

# Tomamos la fuerza que corresponde al promedio de las fuerzas (en caso de existir ambas) o el valore existente entre arcos. 

# In[ ]:

spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatoOrigen/lr-629/riesgo_fraude/contaminacion/contaminados").createOrReplaceTempView("fuerza") 
spark.sql("SELECT * from fuerza where Fi>0").createOrReplaceTempView("fuerza")


spark.sql("SELECT receptor ,(EXP(SUM(LN(NULLIF(ABS(1-Fi),0))))) AS f0 ,sum(Fi) as sFi from  fuerza group by receptor order by receptor asc ").createOrReplaceTempView("fuerza_aux")
# spark.sql("SELECT *  from fuerza_aux").show() 
spark.sql("SELECT emisor,  fuerza.receptor,Fi,f0,sFi from fuerza left join fuerza_aux on fuerza.receptor=fuerza_aux.receptor").createOrReplaceTempView("fuerza")
# spark.sql("SELECT *  from fuerza").show() 
spark.sql("SELECT emisor,receptor, Fi, case when f0 is NULL then 0 else f0 end as f0,sFi from fuerza order by receptor asc").createOrReplaceTempView("fuerza")
# spark.sql("SELECT *  from fuerza").show() 
spark.sql("SELECT emisor,receptor, Fi,f0, round((1-f0)*Fi,4) as f,sFi from fuerza order by receptor asc").createOrReplaceTempView("fuerza")
# spark.sql("SELECT *  from fuerza").show() 
# Ahora se agrega una correcion para dividir fi por el tutal de la suma de Fi. En el caso que la fuerza sea cero, no se realiza la division, pero para los otros casos si )de esta forma nos cercioramos de que sea distinto de cero.
spark.sql("SELECT emisor, receptor, Fi, f0, case when f=0 then f else f/sFi end as f from fuerza order by sFi desc").createOrReplaceTempView("fuerza") 
spark.sql("SELECT * from fuerza order by f desc").show() 


# In[ ]:


spark.sql("SELECT emisor as rut from fuerza").createOrReplaceTempView("a")
spark.sql("SELECT receptor as rut from fuerza").createOrReplaceTempView("b")
spark.sql("SELECT rut FROM a UNION ALL SELECT rut FROM b ORDER BY rut asc").createOrReplaceTempView("c")
spark.sql("SELECT distinct(rut) from c").createOrReplaceTempView("c")
spark.sql("SELECT rut, 0 as score_i from c").createOrReplaceTempView("c")
spark.sql("SELECT count(*) from c").show()


# In[ ]:

df=spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatoOrigen/lr-629/riesgo_fraude/contaminacion/contaminados")
df.createOrReplaceTempView("contaminados")



# In[ ]:


spark.sql("SELECT * from c left join contaminados on c.rut=contaminados.cont_rut").createOrReplaceTempView("contaminados")


# In[ ]:


contaminados=spark.sql("SELECT rut as cont_rut, case when score is not null and score>score_i then score else score_i end as score from contaminados")


# In[ ]:


contaminados.write.mode('overwrite').format("parquet").save(
    "abfs://data@datalakesii.dfs.core.windows.net/DatoOrigen/lr-629/riesgo_fraude/contaminacion/contaminados_processed_iva_representante"
)
# In[ ]:


performance = pd.DataFrame(columns=['iterations', 'new_values'])

for a in range (0,15): 
#   spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatoOrigen/lr-629/riesgo_contaminacion").createOrReplaceTempView("contaminados")
    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatoOrigen/lr-629/riesgo_fraude/contaminacion/contaminados_processed_iva_representante").createOrReplaceTempView("contaminados")
    
    spark.sql("SELECT EMISOR, score as score_socio, RECEPTOR,f0,f from fuerza left join contaminados on fuerza.EMISOR=contaminados.cont_rut").createOrReplaceTempView("aux")
    spark.sql("SELECT * from aux left join contaminados on aux.RECEPTOR=contaminados.cont_rut").createOrReplaceTempView("aux")
    spark.sql("SELECT EMISOR,aux.RECEPTOR as RECEPTOR ,f,f0, score_socio, case when score is null then 0 else score end as score_entidad from aux order by aux.RECEPTOR desc").createOrReplaceTempView("aux")
    spark.sql("SELECT RECEPTOR, SUM(f*score_socio) as sum1, AVG(f0*score_entidad) as sum2 from aux group by RECEPTOR ").createOrReplaceTempView("aux")
    spark.sql("SELECT RECEPTOR, (sum1+sum2) as score1 from aux group ").createOrReplaceTempView("aux")
    spark.sql("SELECT * from contaminados left join aux on contaminados.cont_rut=aux.RECEPTOR").createOrReplaceTempView("aux")
    spark.sql("SELECT aux.cont_rut as cont_rut, case when score1>score and score1 is not null then score1 else score end as score from aux").createOrReplaceTempView("contaminados")
    contaminados=spark.sql("select * from contaminados ")
#   raw_path='abfs://data@datalakesii.dfs.core.windows.net/DatoOrigen/lr-629/riesgo_contaminacion'
#   contaminados.write.format("parquet").mode('overwrite').save(raw_path) 
    contaminados.write.mode('overwrite').format("parquet").save( "abfs://data@datalakesii.dfs.core.windows.net/DatoOrigen/lr-629/riesgo_fraude/contaminacion/contaminados_processed_iva_representante")







