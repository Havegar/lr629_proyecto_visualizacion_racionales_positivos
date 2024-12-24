#!/usr/bin/env python
# coding: utf-8

# ## Apiux & SII: cálculo de índice de familiaridad en personas jurídicas.
# ## ATENCIÓN: proyecto sujeto a mantenimiento continuo.
# ## Henry Vega (henrry.vega@api-ux.com)
# ## Data analyst

import warnings
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
import pyspark

warnings.filterwarnings('ignore', category=DeprecationWarning)

# Configuración de Spark
ss_name = 'Ejecución algoritmo de propagación de materia oscura'
wg_conn = "spark.kerberos.access.hadoopFileSystems"
db_conn = "abfs://data@datalakesii.dfs.core.windows.net/"

# Crear o recuperar una SparkSession con configuración unificada
spark = SparkSession.builder \
    .appName(f"Ejecución algoritmo {ss_name}") \
    .config(wg_conn, db_conn) \
    .config("spark.executor.memory", "6g") \
    .config("spark.driver.memory", "12g") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.instances", "5") \
    .config("spark.driver.maxResultSize", "12g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Configuración adicional para el manejo de Parquet
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

# ## Carga de relaciones societarias (depurada)
df = spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/riesgo_fraude/materia_oscura/malla_procesada")
df.createOrReplaceTempView("sociedad")

# ## Tablas temporales previas a la ejecución
spark.sql("""
    SELECT RUT_SOCIEDAD, 
           RUT_SOCIO 
    FROM sociedad 
    ORDER BY RUT_SOCIEDAD ASC
""").createOrReplaceTempView("sociedad")

spark.sql("""
    SELECT RUT_SOCIEDAD AS RUT_SOCIEDAD_AUX, 
           RUT_SOCIO AS RUT_SOCIO_AUX 
    FROM sociedad 
    ORDER BY RUT_SOCIEDAD ASC
""").createOrReplaceTempView("aux")

# ## Cálculo de árbol de socios naturales
for a in range(1, 10):
    spark.sql("""
        SELECT * 
        FROM sociedad 
        LEFT JOIN aux 
        ON sociedad.RUT_SOCIO = aux.RUT_SOCIEDAD_AUX
    """).createOrReplaceTempView("sociedad")

    spark.sql("""
        SELECT * 
        FROM sociedad 
        ORDER BY RUT_SOCIEDAD_AUX DESC
    """).createOrReplaceTempView("sociedad")

    spark.sql("""
        SELECT RUT_SOCIEDAD, 
               CASE 
                   WHEN RUT_SOCIEDAD_AUX IS NULL THEN RUT_SOCIO 
                   ELSE RUT_SOCIO_AUX 
               END AS RUT_SOCIO 
        FROM sociedad  
        ORDER BY RUT_SOCIEDAD_AUX DESC
    """).createOrReplaceTempView("sociedad")

# ## Carga de datos de oscuridad
oscuridad = spark.sql("""
    SELECT * 
    FROM libsdf.jab_materia_inom
""")
oscuridad.createOrReplaceTempView("oscuridad")

spark.sql("""
    SELECT RUT_SOCIEDAD, 
           CONT_RUT 
    FROM sociedad 
    LEFT JOIN oscuridad 
    ON sociedad.RUT_SOCIO = oscuridad.CONT_RUT
""").createOrReplaceTempView("socios_final")

# ## Sociedades por persona natural
sociedades_por_socio = spark.sql("""
    SELECT CONT_RUT, 
           RUT_SOCIEDAD AS SOCIEDADES_RELACIONADAS 
    FROM socios_final 
    WHERE CONT_RUT IS NOT NULL 
    ORDER BY SOCIEDADES_RELACIONADAS DESC
""")
sociedades_por_socio.write.mode('overwrite').format("parquet").save(
    "abfs://data@datalakesii.dfs.core.windows.net/DatoOrigen/lr-629/riesgo_fraude/materia_oscura/sociedades_por_socio"
)

# ## Cálculo de métricas de sociedades
spark.sql("""
    SELECT RUT_SOCIEDAD, 
           COUNT(*) AS NONULOS 
    FROM socios_final 
    WHERE CONT_RUT IS NOT NULL 
    GROUP BY RUT_SOCIEDAD
""").createOrReplaceTempView("nonulos")

spark.sql("""
    SELECT RUT_SOCIEDAD, 
           COUNT(*) AS TOTAL 
    FROM socios_final 
    GROUP BY RUT_SOCIEDAD
""").createOrReplaceTempView("total")

spark.sql("""
    SELECT total.RUT_SOCIEDAD AS RUT_SOCIEDAD, 
           TOTAL, 
           NONULOS 
    FROM total 
    LEFT JOIN nonulos 
    ON total.RUT_SOCIEDAD = nonulos.RUT_SOCIEDAD
""").createOrReplaceTempView("final")

# Filtrar sociedades con todas las personas naturales completas y menos de 10,000 personas naturales conectadas
spark.sql("""
    SELECT socios_final.RUT_SOCIEDAD, 
           CONT_RUT AS RUT_SOCIO 
    FROM socios_final 
    LEFT JOIN final 
    ON socios_final.RUT_SOCIEDAD = final.RUT_SOCIEDAD 
    WHERE TOTAL = NONULOS 
      AND TOTAL <= 10000 
    ORDER BY TOTAL DESC
""").createOrReplaceTempView("final")

# ## Combinatoria de pool de socios
spark.sql("""
    SELECT t1.RUT_SOCIEDAD, 
           t1.RUT_SOCIO AS RUT_SOCIO_1, 
           t2.RUT_SOCIO AS RUT_SOCIO_2 
    FROM final AS t1 
    JOIN final AS t2 
    ON t1.RUT_SOCIEDAD = t2.RUT_SOCIEDAD  
    WHERE t1.RUT_SOCIO <> t2.RUT_SOCIO
""").createOrReplaceTempView("final")

spark.sql("""
    SELECT RUT_SOCIEDAD, 
           RUT_SOCIO_1, 
           RUT_SOCIO_2, 
           RUT_SOCIO_1 || RUT_SOCIO_2 AS key 
    FROM final
""").createOrReplaceTempView("final")

# ## Exploración, limpieza y ampliación de datos de relaciones familiares
spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/LibSDF/REL_FAMILIARES_AARI_EH").createOrReplaceTempView("familiar")

# Filtrar relaciones familiares repetidas y eliminar relaciones donde CONT_RUT sea igual a RUT_FAM
spark.sql("""
    SELECT CONT_RUT, 
           RUT_FAM, 
           COUNT(*) AS c 
    FROM familiar 
    WHERE CONT_RUT != RUT_FAM 
    GROUP BY CONT_RUT, RUT_FAM 
    ORDER BY c DESC
""").createOrReplaceTempView("familiar")

spark.sql("""
    SELECT CONT_RUT, 
           RUT_FAM 
    FROM familiar
""").createOrReplaceTempView("familiar")

# Duplicar y invertir las relaciones familiares para considerar la bidireccionalidad
spark.sql("""
    SELECT CONT_RUT AS RUT_FAM, 
           RUT_FAM AS CONT_RUT 
    FROM familiar
""").createOrReplaceTempView("familiar2")

spark.sql("""
    SELECT * 
    FROM familiar 
    UNION ALL 
    SELECT * 
    FROM familiar2
""").createOrReplaceTempView("familiar")

spark.sql("""
    SELECT CONT_RUT, 
           RUT_FAM, 
           COUNT(*) AS C 
    FROM familiar 
    GROUP BY CONT_RUT, RUT_FAM 
    ORDER BY C DESC
""").createOrReplaceTempView("familiar")

spark.sql("""
    SELECT CONT_RUT || RUT_FAM AS key 
    FROM familiar
""").createOrReplaceTempView("familiar")

# ## Cruce de relaciones familiares calculadas con el pool de socios por sociedad
spark.sql("""
    SELECT RUT_SOCIEDAD, 
           RUT_SOCIO_1, 
           RUT_SOCIO_2, 
           familiar.key AS FAMILIAR_KEY 
    FROM final 
    LEFT JOIN familiar 
    ON final.key = familiar.key 
    ORDER BY RUT_SOCIEDAD
""").createOrReplaceTempView("relaciones")

# Obtener los valores únicos de socios que tienen relaciones familiares
spark.sql("""
    SELECT RUT_SOCIEDAD, 
           COUNT(DISTINCT(RUT_SOCIO_1)) AS FAMILIARES 
    FROM relaciones 
    WHERE FAMILIAR_KEY IS NOT NULL 
    GROUP BY RUT_SOCIEDAD
""").createOrReplaceTempView("socios_familia")

# ## Cálculo del índice de familiaridad
spark.sql("""
    SELECT total.RUT_SOCIEDAD, 
           CASE 
               WHEN FAMILIARES IS NULL THEN 0 
               ELSE FAMILIARES 
           END AS FAMILIARES, 
           TOTAL 
    FROM total 
    LEFT JOIN socios_familia 
    ON socios_familia.RUT_SOCIEDAD = total.RUT_SOCIEDAD
""").createOrReplaceTempView("socios_familia")

df = spark.sql("""
    SELECT RUT_SOCIEDAD, 
           FAMILIARES, 
           TOTAL, 
           FAMILIARES / TOTAL * 100 AS TASA_FAMILIARIDAD 
    FROM socios_familia
""")

# Guardar el resultado en la carpeta de Azure del proyecto
df.write.mode('overwrite').format("parquet").save(
    "abfs://data@datalakesii.dfs.core.windows.net/DatoOrigen/lr-629/riesgo_fraude/materia_oscura/familiaridad"
)


