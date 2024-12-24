#!/usr/bin/env python
# coding: utf-8

# ## Apiux & SII: calculo de indice de materia oscura en personas juridicas.
# ## ATENCION: proyecto sujeto a mantenimiento continuo. 
# 
# ## Henry Vega (henrry.vega@api-ux.com)
# ## Data analyst

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
import pyspark
#warnings.filterwarnings('ignore', category=DeprecationWarning)

ss_name = 'Ejecucion algoritmo de propagacion de materia oscura'
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


# ## Carga de relaciones societarias(depurada)

# Se carga la data depurada anteriormente de relaciones societarias.

# In[3]:


df = spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/riesgo_fraude/materia_oscura/malla_procesada")

df.createOrReplaceTempView("sociedad")


# ## Exploracion de composiciones de socios en capital y utilidades

# Como no hay unicida de entradas y se puede presentar varias veces una combinacion sociedad y socio, se agrupa por el promedio para cada relacion. De esta forma analizamos la composicion de la sociedad en utilidades y capital.
# 

# In[4]:


spark.sql("select  RUT_SOCIEDAD, RUT_SOCIO, mean(PORCENTAJE_CAPITAL) as PORCENTAJE_CAPITAL, mean(PORCENTAJE_UTILIDADES) as PORCENTAJE_UTILIDADES from sociedad group by RUT_SOCIEDAD, RUT_SOCIO").createOrReplaceTempView("composicion")
spark.sql("select  RUT_SOCIEDAD, sum(PORCENTAJE_CAPITAL) as TOTAL_CAPITAL, SUM(PORCENTAJE_UTILIDADES) as TOTAL_UTILIDADES from composicion group by RUT_SOCIEDAD").createOrReplaceTempView("composicion")
m=spark.sql("select * from composicion").toPandas()

# Se utilizara el porcentaje de capital en lugar de utilidades para hacer la transmision de materia oscura. En el proceso de limpieza de datos se considero las filas con valores no nuels de PORCENTAJE_CAPITAL. Se utilizara una tabla de composicion para poder normalizar los valores de oscuridad para cuando los valores de porcentajes se capital sean diferentes al 100%.

# ## Tabla de ponderacion para el total de capital.


spark.sql("select RUT_SOCIEDAD as CONT_RUT, TOTAL_CAPITAL as ponderador from composicion").createOrReplaceTempView("composicion")


# ## Lectura de tabla de oscuridad

# Se hace lectura de los datos de iniciales de oscuridad para personas naturales.


oscuridad1=spark.sql("select * from libsdf.jab_materia_inom")
oscuridad1.createOrReplaceTempView("oscuridad")
spark.sql("select * from oscuridad").show()


# ## Ajuste de valores de participacion societaria.

# Como para una combinacion sociedad socio tenemos distintos valores de PORCENTAJE_CAPITAL y PORCENTAJE_UTILIDADES se usara el promedio de dichos valores.

# In[11]:


spark.sql("select  RUT_SOCIEDAD, RUT_SOCIO, mean(PORCENTAJE_CAPITAL) as PORCENTAJE_CAPITAL, mean(PORCENTAJE_UTILIDADES) as PORCENTAJE_UTILIDADES from sociedad group by RUT_SOCIEDAD, RUT_SOCIO").createOrReplaceTempView("sociedad")


# ## Primera iteracion

# Se realiza el cruce de la data societaria con la data de  oscuridad de personas naturales (1re paso de calculo de materia oscura para sociedades completas). Para ello se completa la malla con la oscuridad inicial. Hay sociedades donde no se completan todos los socios con oscuridad, por lo cual se discrimina mediante un contador de nulos de dicho campo para poder agregar la data y obtener un valor de oscuridad para un rut de soiedad donde todas las entradas de sus socios han sido completadas.

# In[12]:


#Iteracion 0
spark.sql("select * from sociedad left join oscuridad on sociedad.RUT_SOCIO=oscuridad.CONT_RUT order by sociedad.RUT_SOCIEDAD asc").createOrReplaceTempView("sociedad")
#spark.sql("select * from sociedad ").show()
spark.sql("select RUT_SOCIEDAD, RUT_SOCIO, PORCENTAJE_CAPITAL, Valor from sociedad").createOrReplaceTempView("sociedad")
#spark.sql("select * from sociedad ").show()
spark.sql("select RUT_SOCIEDAD as RUT_SOCIEDAD1, count(*) as nulos  from sociedad where Valor is null group by RUT_SOCIEDAD order by RUT_SOCIEDAD ASC").createOrReplaceTempView("aux")
#spark.sql("select * from aux ").show()
spark.sql("select RUT_SOCIEDAD,RUT_SOCIO,PORCENTAJE_CAPITAL, nulos, Valor from sociedad left join aux on sociedad.RUT_SOCIEDAD=aux.RUT_SOCIEDAD1 order by RUT_SOCIEDAD asc ").createOrReplaceTempView("aux")
#spark.sql("select * from aux where nulos is null ").show()


# ## Primera iteracion, cruce con oscuridad de personas naturales (2do paso de calculo de oscuridad y ponderacion por total de capital)

# Junto con completar la data con los valores de oscuridad para sociedades completas, se agrega un ponderador. De esta forma, si una entidad tiene dos socios con oscuridad y la composicion suma diferente a 100%, se pondera proporcionalmente para que sumen 100%.

# In[13]:


#agregar a la oscuridad la ponderacion adecuada

spark.sql("select RUT_SOCIEDAD,RUT_SOCIO,PORCENTAJE_CAPITAL, Valor from aux where nulos is null").createOrReplaceTempView("aux")
#spark.sql("select * from aux").show()
spark.sql("select RUT_SOCIEDAD as CONT_RUT, SUM(PORCENTAJE_CAPITAL*Valor*0.01) as othervalue from aux group by RUT_SOCIEDAD").createOrReplaceTempView("oscuridad")
#spark.sql("select * from oscuridad").show()
spark.sql("select CONT_RUT, othervalue as Value from oscuridad").createOrReplaceTempView("oscuridad")
#spark.sql("select * from oscuridad").show()

spark.sql("select oscuridad.CONT_RUT as CONT_RUT, Value, ponderador from oscuridad left join composicion on oscuridad.CONT_RUT=composicion.CONT_RUT order by oscuridad.CONT_RUT desc ").createOrReplaceTempView("oscuridad")
#spark.sql("select * from oscuridad ").show()
spark.sql("select CONT_RUT, Value/ponderador*100 as Value from oscuridad ").createOrReplaceTempView("oscuridad")

#Ahora guardaremos la tabla oscuridad en una tabla auxiliar para poder ir guardando los valores sin excederse en los recursos

#spark.sql("SELECT * FROM oscuridad").write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatoOrigen/lr-629/Oscuridad/intermedia/oscuridad_aux")

# Guardar oscuridad_aux en el Data Lake como Parquet
spark.sql("SELECT * FROM oscuridad").write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/riesgo_fraude/materia_oscura/oscuridad_aux")

try:
    a=spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/riesgo_fraude/materia_oscura/oscuridad_aux")
    a.limit(10).show()
                  #.registerTempTable('tmp_historico')
    print("Archivo cargado y tabla temporal registrada exitosamente.")
except AnalysisException as e:
    if 'Path does not exist' in str(e):
        print("El archivo no está disponible en la ruta especificada.")

    else:
        print("Ocurrió un error al intentar leer el archivo:", str(e))

#spark.sql("select RUT_SOCIEDAD,RUT_SOCIO,PORCENTAJE_UTILIDADES, Valor from aux where nulos is null").createOrReplaceTempView("aux")

oscuridad_aux=spark.sql("select * from oscuridad ").toPandas()
oscuridad_aux['iterations']=0


# ## Primera iteracion,resultados

# Principales resultados de la primera iteracion, es decir la iteracion cero. 

# In[14]:


oscuridad_aux.describe()


# ## Iteraciones subsiguientes

# Se repite el proceso iterativo para asi completar mas sociedades. El parametro *iter* hara referencia al numero de iteraciones luego de la iteracion 0.

# In[15]:


iter=10


# In[16]:


for iteration in range(1,iter):
    #spark.sql("REFRESH TABLE oscuridad;")
    print("iteracion: ", iteration)
    if iteration == 1 :
        temp = spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/riesgo_fraude/materia_oscura/oscuridad_aux")
        temp.limit(10).show()
        print("Lectura de tabla oscuridad_aux completa")
    else     :
        vuelta=iteration
        temp = spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/riesgo_fraude/materia_oscura/oscuridad_aux_"+str(vuelta))
        temp.limit(10).show()
        print("Lectura de tabla oscuridad_aux completa")
    
    temp.createOrReplaceTempView("oscuridad")                               
   # spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatoOrigen/lr-629/Oscuridad/intermedia/oscuridad_aux").createOrReplaceTempView("oscuridad")
    
    spark.sql("select * from sociedad left join oscuridad on sociedad.RUT_SOCIO=oscuridad.CONT_RUT order by sociedad.RUT_SOCIEDAD asc").createOrReplaceTempView("sociedad")
    spark.sql("select RUT_SOCIEDAD, RUT_SOCIO, PORCENTAJE_CAPITAL, CASE WHEN Valor is null THEN Value ELSE Valor END AS Valor from sociedad").createOrReplaceTempView("sociedad")
    spark.sql("select RUT_SOCIEDAD, RUT_SOCIO, PORCENTAJE_CAPITAL, Valor from sociedad").createOrReplaceTempView("sociedad")
    spark.sql("select RUT_SOCIEDAD as RUT_SOCIEDAD1, count(*) as nulos from sociedad where Valor is null group by RUT_SOCIEDAD order by RUT_SOCIEDAD ASC").createOrReplaceTempView("aux")
    spark.sql("select RUT_SOCIEDAD,RUT_SOCIO,PORCENTAJE_CAPITAL, nulos, Valor from sociedad left join aux on sociedad.RUT_SOCIEDAD=aux.RUT_SOCIEDAD1 order by RUT_SOCIEDAD asc ").createOrReplaceTempView("aux")
    spark.sql("select RUT_SOCIEDAD,RUT_SOCIO,PORCENTAJE_CAPITAL, Valor from aux where nulos is null").createOrReplaceTempView("aux")
    spark.sql("select RUT_SOCIEDAD as CONT_RUT, SUM(PORCENTAJE_CAPITAL*Valor*0.01) as othervalue from aux group by RUT_SOCIEDAD").createOrReplaceTempView("oscuridad")
    spark.sql("select CONT_RUT, othervalue as Value from oscuridad").createOrReplaceTempView("oscuridad")
    
    spark.sql("select oscuridad.CONT_RUT as CONT_RUT, Value,ponderador from oscuridad left join composicion on oscuridad.CONT_RUT=composicion.CONT_RUT order by oscuridad.CONT_RUT asc ").createOrReplaceTempView("oscuridad")
    #spark.sql("select * from oscuridad ").show()
    spark.sql("select CONT_RUT, Value/ponderador*100 as Value from oscuridad order by Value desc ").createOrReplaceTempView("oscuridad")

    
    #spark.sql("SELECT * FROM oscuridad").write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatoOrigen/lr-629/Oscuridad/intermedia/oscuridad_aux")
    print("Inicio oscuridad procesada")
    vuelta_sigte=iteration+1
    spark.sql("SELECT * FROM oscuridad").limit(10).show()
    spark.sql("SELECT * FROM oscuridad").write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/riesgo_fraude/materia_oscura/oscuridad_aux_"+str(vuelta_sigte))
    
    oscuridad=spark.sql("select * from oscuridad").toPandas()
    oscuridad['iterations']=iteration

    oscuridad=oscuridad.merge(oscuridad_aux, on = "CONT_RUT", how = "left")
    oscuridad['iterations']=oscuridad[["iterations_x", "iterations_y"]].min(axis=1)
    oscuridad = oscuridad.rename(columns={'Value_x': 'Value'})
    oscuridad=oscuridad[['CONT_RUT','Value','iterations']]
    oscuridad_aux=oscuridad.iloc[:,:]
    print(oscuridad_aux.describe())


# ## Resultados y guardado en archivo csv

# Luego de estas iteraciones, se obtienen dos outputs. En primer lugar una tabla actualizada de sociedades y socios con cada valor calculado (o no) de la oscuridad de sus socios. 
# En segundo lugar la oscuridad calculada para sociedades.

# Convertir 'oscuridad_aux' a una lista de filas (tuplas)
rows = [tuple(x) for x in oscuridad_aux.to_numpy()]

# Crear un DataFrame de Spark a partir de las filas

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Especificar el esquema manualmente
schema = StructType([
    StructField("CONT_RUT", StringType(), True),
    StructField("Value", DoubleType(), True),
    StructField("iterations", DoubleType(), True)
])
oscur_aux = spark.createDataFrame(rows, schema=schema)

oscur_aux.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatoOrigen/lr-629/Oscuridad/final/oscuridad")

# In[17]:
sociedad=spark.sql("select * from sociedad")
sociedad=sociedad.toPandas()


# In[18]:


# Se guardan los archivos finales en el espacio de CML
sociedad.to_csv('artefactos/Oscuridad/Sociedades_oscuridad_actualizada.csv', index=False)
oscuridad_aux.to_csv('artefactos/Oscuridad/Sociedades_oscuridad_completa.csv', index=False)

#guardamos 
df=spark.sql("select * from sociedad")
df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatoOrigen/lr-629/UtilBajo/intermedia/oscuridad_sociedad")


# In[ ]: