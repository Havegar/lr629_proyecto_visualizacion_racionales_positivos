#!/usr/bin/env python
# coding: utf-8

# #### Actualización de proyecto: Cálculo de remanente de crédito contaminado.
# Actualizado para primer periodo de caluclo historico. 
# Se agrega la contaminacion a causa de la emision de documentos de tipo 30 sin autorización.

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
import pyspark
import pandas as pd
from pyspark.sql.utils import AnalysisException


# In[2]:


ss_name = 'Ejecucion algoritmo IVA Credito Remanente'
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


# In[3]:


#spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen//DW/DW_TRN_F29_E").registerTempTable('tmp_f29_0')
#spark.sql("select * from dw.dw_trn_f29_e limit 1").show()


# In[4]:
from datetime import datetime, timedelta
def generate_periods_from_today_as_integers(num_years):
    
    # Obtener la fecha actual
    today = datetime.today()
    
    # Inicializar la lista de periodos
    periods = []
    
    # Fecha de inicio es el primer día del mes actual
    start_date = today.replace(day=1)
    
    # Calcular la fecha final (hace num_years años)
    end_date = start_date - timedelta(days=num_years * 365)
    
    # Generar periodos en formato YYYYMM
    current_date = start_date
    while current_date >= end_date:
        # Convertir el periodo a entero y añadir a la lista al inicio
        period_as_int = int(current_date.strftime("%Y%m"))
        periods.insert(0, period_as_int)  # Inserta al principio de la lista
        # Mover al mes anterior
        next_month = current_date.month - 1 if current_date.month > 1 else 12
        next_year = current_date.year - 1 if current_date.month == 1 else current_date.year
        current_date = current_date.replace(year=next_year, month=next_month, day=1)
    
    return periods

# Ejemplo de uso
num_years = 3
periods_list = generate_periods_from_today_as_integers(num_years)
print(periods_list)

#periods_list=[202301]

iter=0
for Periodo in periods_list:
    print(f"INICIO DE PERIODO {Periodo}")

    Periodo_Ant = str(Periodo)

    if Periodo_Ant[-2:] == '01':
        # Si el periodo es el primero del año (Enero), ajusta al último mes del año anterior
        Periodo_Ant = str(int(Periodo_Ant[:4]) - 1) + "12"
    else:
        # De lo contrario, solo resta 1 al periodo actual
        Periodo_Ant = str(Periodo - 1).zfill(6)  # Asegura que el formato sea correcto (6 dígitos)

    # Convierte Periodo_Ant a entero
    Periodo_Ant = int(Periodo_Ant)
    print(Periodo_Ant)


    # In[5]:
    exists_hist=0
    if iter!=0:
        #tmp_historico: Carga de REsultados histórico
        try:
            #spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/historico_updated")\
                  #.registerTempTable('tmp_historico')
            print("Archivo cargado y tabla temporal registrada exitosamente.")
            exists_hist=1
        except AnalysisException as e:
            if 'Path does not exist' in str(e):
                print("El archivo no está disponible en la ruta especificada.")
                exists_hist=0
            else:
                print("Ocurrió un error al intentar leer el archivo:", str(e))
    iter=iter+1

    # In[6]:


    if exists_hist==1:
        df=spark.sql("select * from tmp_historico")
        df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/historico_resp")
        print("respaldo de tmp_historico ok")



    #Quida duplicados
    df=spark.sql(f"select max(F29_HFRM_FOLIO_vo) as max_folio, cont_rut,f29_agno_mes_tributario_vo as periodo\
               from dw.dw_trn_f29_e \
               where left(f29_agno_mes_tributario_vo,6) in ({Periodo_Ant}) \
              and  TIVA_COD_VALIDEZ  in( 1,3) \
              and  F29_c_537 > 0\
              group by cont_rut,f29_agno_mes_tributario_vo")

    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/f29_unico")
    print("tmp_f29_unico_ant ok")


    # In[9]:


    #tmp_f29_1: F29 del periodo
    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/f29_unico")\
         .registerTempTable('tmp_f29_unico')


    # In[10]:


    # Paso 1
    # Declaraciones de F29 del periodo tributario anterior Periodo_Ant 
    df=spark.sql(f"select distinct t1.cont_rut,t1.f29_agno_mes_tributario_vo as periodo_ant,\
                      F29_c_537 as f29_537_ant, \
                      coalesce(F29_c_538,0) as f29_538_ant,\
                      coalesce(f29_c_77,0) as f29_77_ant,\
                      coalesce(F29_768,0)+coalesce(F29_767,0) as monto_cr_especial_ant,\
                      round(coalesce(f29_c_77,0)/(F29_c_537+ coalesce(F29_725,0)+coalesce(F29_c_704,0)+\
                      coalesce(F29_c_160,0)+coalesce(F29_C_126,0)+coalesce(F29_C_572,0)+\
                      coalesce(F29_768,0)+coalesce(F29_767,0)),6) as porc_remanente_ant\
               from dw.dw_trn_f29_e as t1\
               inner join tmp_f29_unico as t2 on t1.F29_HFRM_FOLIO_vo = t2.max_folio\
               where left(f29_agno_mes_tributario_vo,6) in ({Periodo_Ant}) \
               and   F29_c_537 > 0\
              and  TIVA_COD_VALIDEZ  in( 1,3) ")

    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/f29_ant")

    #31.378.203
    #202112: 1.565.882
            #5.282.343
    print("tmp_f29_ant ok")


    # In[11]:


    #tmp_f29_ant': F29 del periodo ANTERIOR
    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/f29_ant")\
              .registerTempTable('tmp_f29_ant')


    # In[12]:


    Periodo_Ant


    # In[13]:


    #Quida duplicados
    df=spark.sql(f"select max(F29_HFRM_FOLIO_vo) as max_folio, cont_rut,f29_agno_mes_tributario_vo as periodo\
               from dw.dw_trn_f29_e \
               where left(f29_agno_mes_tributario_vo,6) in ({Periodo}) \
              and  TIVA_COD_VALIDEZ  in( 1,3) \
              and  F29_c_537 > 0\
              group by cont_rut,f29_agno_mes_tributario_vo")

    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/f29_unico")

    print("tmp_f29_unico ok")


    # In[14]:


    #tmp_f29_1: F29 del periodo
    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/f29_unico")\
         .registerTempTable('tmp_f29_unico')


    # In[15]:


    # Paso 2
    # Declaraciones de F29 del periodo tributario Actual
    # se agrega código 504 de Remanente periodo anterior ajustado
    df=spark.sql(f"select distinct t1.cont_rut,\
                      F29_c_502 as monto_fact, F29_c_503 as qty_fact, F29_c_519, \
                      F29_c_520,  t1.f29_agno_mes_tributario_vo as periodo,\
                      F29_762 as fact_re_sup, F29_766 as fact_re_const, F29_c_525 as fact_re_act,coalesce(F29_c_528,0) as nc_re,\
                      coalesce(F29_c_520,0) +coalesce(F29_762,0)+coalesce(F29_766,0)+coalesce(F29_c_525,0) as monto_fact_re,  \
                      F29_c_538, f29_c_509 as qty_nc, f29_c_510 as monto_nc,\
                      F29_c_537 as F29_c_537, \
                      coalesce(F29_c_520,0) +coalesce(F29_762,0)+coalesce(F29_766,0)+coalesce(F29_c_525,0)+coalesce(F29_c_504,0) - \
                      coalesce(F29_c_528,0) + coalesce(F29_c_532,0)+coalesce(F29_c_535,0) + coalesce(F29_c_553,0) as monto_neto_re,\
                      coalesce(F29_725,0)+coalesce(F29_c_704,0)+\
                      coalesce(F29_c_160,0)+coalesce(F29_C_126,0)+coalesce(F29_C_572,0)+\
                      coalesce(F29_768,0)+coalesce(F29_767,0) as monto_cr_especial,\
                      coalesce(F29_c_504,0) as F29_c_504\
               from dw.dw_trn_f29_e as t1\
               inner join tmp_f29_unico as t2 on t1.F29_HFRM_FOLIO_vo = t2.max_folio\
               where left(f29_agno_mes_tributario_vo,6) in ({Periodo}) \
              and  TIVA_COD_VALIDEZ in( 1,3) \
              and  F29_c_537 > 0")

    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/f29_0")


    # In[16]:


    #tmp_f29_1: F29 del periodo
    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/f29_0")\
         .registerTempTable('tmp_f29_0')


    # In[17]:


    #Contribuyentes  que presentan Observacione FM01
    df=spark.sql(f" select cont_rut,PERI_AGNO_MES,INF3_OBSC_DISCRIMINANTE \
                               from DW.DW_TRN_IVAPRO_INF3_VIG_e as t2 \
                               where t2.peri_agno_mes in ({Periodo}) \
                               and t2.inf3_obsr_codigo = 'FM01'\
                               and INF3_FECHA_FIN_OBS_DW is null\
                               and  INF3_ESTADO_VIGENCIA='V'")
    #df.registerTempTable('tmp_obsfm01') 
    #202112:
    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/tmp_obsfm01")


    # In[18]:


    #tmp_f29_1: F29 del periodo
    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/tmp_obsfm01")\
         .registerTempTable('tmp_obsfm01_1')


    # In[19]:


    df=spark.sql("select t1.cont_rut,\
                      monto_fact, qty_fact, F29_c_519, \
                      F29_c_520,  periodo,\
                      fact_re_sup, fact_re_const, fact_re_act, nc_re,\
                      monto_fact_re,  \
                      F29_c_538, qty_nc, monto_nc,\
                      F29_c_537, \
                      monto_neto_re,\
                      monto_cr_especial,\
                      t1.F29_c_504 as F29_c_504_orig,\
                          case when coalesce(t1.F29_c_504,0) -coalesce(INF3_OBSC_DISCRIMINANTE,0) < 0 then 0 \
                               else coalesce(t1.F29_c_504,0) -coalesce(INF3_OBSC_DISCRIMINANTE,0) end as F29_c_504,\
                          case when coalesce(t1.F29_c_504,0) -coalesce(INF3_OBSC_DISCRIMINANTE,0) < 0 then coalesce(t1.F29_c_504,0) \
                               else coalesce(INF3_OBSC_DISCRIMINANTE,0) end as fm01\
                  from tmp_f29_0 as t1 \
                  left join tmp_obsfm01_1 as t2 on t1.cont_rut = t2.cont_rut")

    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/f29_1")


    # In[20]:


    #tmp_f29_1: F29 del periodo
    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/f29_1")\
         .registerTempTable('tmp_f29_1')


    # In[21]:


    if exists_hist==1:
        spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/remanente_ant_updated")\
         .registerTempTable('tmp_remanente_ant')


    # In[22]:


    if exists_hist==1:
        # Resultados del perodo antrior
        df=spark.sql(f"select dcv_ptributario as periodo_ant, receptor,{Periodo} as periodo,\
                          total_cont_an as total_cont_an_ant,\
                          case when coalesce(prop_total_cont_an,0) < 0 then 0 else  coalesce(prop_total_cont_an,0) end as prop_total_cont_an_ant, \
                          t2.f29_77_ant,\
                          F29_c_504_orig,\
                          F29_c_504,\
                          fm01,\
                          round(case when coalesce(prop_total_cont_an,0) < 0 then 0 else  coalesce(prop_total_cont_an,0) end * coalesce(t3.F29_c_504,0),4) as prop_remanente_cont\
                   from tmp_historico as t1\
                   left join tmp_f29_ant as t2 on t1.receptor = t2.cont_rut\
                   left join tmp_f29_1 as t3 on t1.receptor = t3.cont_rut\
                   where dcv_ptributario = {Periodo_Ant}")

        df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/remanente_ant_updated")


    if exists_hist==0:
        # Resultados del perodo antrior
        df=spark.sql(f"select {Periodo_Ant} as periodo_ant, t2.cont_rut as receptor,{Periodo} as periodo,\
                          0 as total_cont_an_ant,\
                          0 as prop_total_cont_an_ant, \
                          t2.f29_77_ant,\
                          F29_c_504_orig,\
                          F29_c_504,\
                          fm01,\
                          0 as prop_remanente_cont\
                   from tmp_f29_1 as t1\
                   left join tmp_f29_ant as t2 on t1.cont_rut = t2.cont_rut\
                   where 1=1")

        df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/remanente_ant_updated")

    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/remanente_ant_updated")\
         .registerTempTable('tmp_remanente_ant')

    # In[23]:


    # spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/remanente_ant").registerTempTable('tmp_remanente_ant')
    # spark.sql("select * from tmp_remanente_ant limit 20").show()


    # In[24]:


    # df=spark.sql("select dcv_ptributario, receptor,total_cont_an,prop_total_cont_an,\
    #                      prop_total_cont_an \
    #               from tmp_historico_e limit 10")
    # pd=df.toPandas()

    #exists_hist


    # In[25]:


    #Paso 2
    # Documentos de RCV
# Paso 2
# Documentos de RCV
    df=spark.sql(f"select dhdr_codigo, dcv_ptributario,det_tipo_doc,dcv_rut_emisor_e,det_rut_doc_e,tipo_transaccion,dcv_operacion,\
                        det_mnt_iva\
               from dwbgdata.dcv_generic_det_consolidado_sas\
               where dcv_ptributario in ({Periodo})\
               and   tipo_transaccion in(1,2,3,4,5,6) \
               and   det_tipo_doc IN (30, 33, 55, 56, 60, 61)\
               and   dcv_operacion = 'COMPRA'")

    df.registerTempTable('tmp_cv')
    #39.903.589
    #39.740.811


    # In[26]:


    #spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/HADOOP/DTE/dcv_gen_det_consol").registerTempTable('tmp_cv')


    # In[27]:


    # Paso 2a
    # temporal de documentos de compra.
    df=spark.sql(f"select t2.dhdr_codigo, t2.dcv_ptributario, \
                      case when t2.det_tipo_doc IN (29, 30, 33, 45, 46, 55, 56) then t2.det_mnt_iva else 0 end as iva_fact,\
                      case when t2.det_tipo_doc in(60, 61) then t2.det_mnt_iva else 0 end as iva_nc,\
                      case when t2.det_tipo_doc IN (29, 30, 33, 45, 46, 55, 56) then 1 else 0 end as es_fact,\
                      case when t2.det_tipo_doc in(60, 61) then 1 else 0 end as es_nc,\
                      t2.dcv_rut_emisor_e as receptor,t2.det_tipo_doc, t2.det_rut_doc_e, \
                      case when t2.det_tipo_doc in(60, 61) then t2.det_mnt_iva * -1 else t2.det_mnt_iva end as monto_iva,dcv_rut_emisor_e\
               from tmp_cv as t2 \
               where t2.dcv_ptributario in ({Periodo})\
               and   t2.tipo_transaccion in(1,2,3,4,5,6) \
               and   det_tipo_doc IN (30, 33, 55, 56, 60, 61)\
               and   t2.dcv_operacion = 'COMPRA'")

    df.registerTempTable('tmp_docto')
    print("tmp_docto ok")
    #34.955.891
    #39.903.589
    #39.740.811


    # In[28]:


    # Paso 2c (Antes se debe ejecutar Paso 2)
    # temporal de declaración de documentos
    # tipo de documento declarado= 33:Factura ; 61: Nota de Crédito.
    df=spark.sql("select det_rut_doc_e,\
                      sum(iva_fact) as iva_fact,\
                      sum(iva_nc) as iva_nc,\
                      receptor,\
                      dcv_ptributario,\
                      count(*) as qty_docto,\
                      sum(es_fact) as qty_factura,\
                      sum(es_nc) as qty_nc,\
                        sum(coalesce(iva_fact,0)) - sum(coalesce(iva_nc,0)) as iva_neto1, \
                        sum(coalesce(monto_iva,0)) as iva_neto_cv1,\
                        dcv_rut_emisor_e,\
                        case when length(t2.cont_rut)>1  then 1 else 0 end as emis_declara_f29,\
                        case when length(t3.cont_rut) > 1 then 1 else 0 end as rece_declara_f29\
               from tmp_docto as t1\
               left join tmp_f29_1 as t2 on t1.det_rut_doc_e = t2.cont_rut and t1.dcv_ptributario = t2.periodo\
               left join tmp_f29_1 as t3 on t1.receptor = t3.cont_rut and t1.dcv_ptributario = t3.periodo\
               where 1=1\
               group by det_rut_doc_e,\
                      receptor,dcv_ptributario,dcv_rut_emisor_e,t2.cont_rut,t3.cont_rut")




    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/tmp_res_docto01")

    #202112: 13.497.720
    #        14.687.488
    #2021: 173.021.529


    # In[29]:


    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/tmp_res_docto01")\
         .registerTempTable('tmp_res_docto01')


    # In[30]:


    df=spark.sql("select det_rut_doc_e,iva_fact,iva_nc,\
                    receptor,dcv_ptributario,qty_docto,\
                    qty_factura,qty_nc,\
                    case when iva_neto1 < 0 then 0 else iva_neto1 end as iva_neto,\
                    case when iva_neto_cv1 < 0 then 0 else iva_neto_cv1 end as iva_neto_cv,\
                    dcv_rut_emisor_e,emis_declara_f29,rece_declara_f29\
                from tmp_res_docto01")

    df.registerTempTable('tmp_res_docto0')

    print("tmp_res_docto0 ok")



    # In[31]:


    #spark.sql("select * from tmp_res_docto0 where dcv_rut_emisor_e = 'lnmGCScjrf/x9Ief7uzgAQ=='").show()

    # df=spark.sql("select * from tmp_f29_1 where cont_rut = 'lnmGCScjrf/x9Ief7uzgAQ=='")
    # pd=df.toPandas()
    # pd


    # In[32]:


    df=spark.sql("select *, 'f29' as tipo \
               from tmp_f29_1\
               union\
               select receptor, 0,0,0,0,dcv_ptributario,0,0,0,sum(iva_nc),sum(iva_fact),\
                     0,0,0,sum(iva_neto),sum(iva_neto),0,0,'rcv',0,0\
               from tmp_res_docto0 as t1\
               where not exists(select *\
                                from   tmp_f29_1 as t2\
                                where  t2.cont_rut = t1.dcv_rut_emisor_e\
                                and    t2.periodo = t1.dcv_ptributario )\
               group by receptor,dcv_ptributario ")

    #df.registerTempTable('tmp_f29_0')
    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/f29_0")


    # In[33]:


    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/f29_0")\
         .registerTempTable('tmp_f29_0')


    # In[34]:


    df=spark.sql("select cont_rut, monto_fact,qty_fact,F29_c_519,\
                        F29_c_520,periodo,fact_re_sup,fact_re_const,fact_re_act,\
                        nc_re,monto_fact_re,  F29_c_538,qty_nc,\
                        monto_nc,F29_c_537,\
                        case when monto_neto_re < 0 then 0 else monto_neto_re end as monto_neto_re,\
                        monto_cr_especial,F29_c_504,tipo\
                  from tmp_f29_0")

    #df.registerTempTable('tmp_f29')

    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/f29")

    #4.284.183
    #4.284.173


    # In[35]:


    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/f29").registerTempTable('tmp_f29')
    print("tmp_f29 ok")


    # In[36]:


    # Paso 3
    # Extrae documentos de los RUT con documentos electronicos que no presentan declaración en F29
    # Falta agregar condición que considera los contribuyentes que presentan observación W01
    df=spark.sql("select det_rut_doc_e, dcv_ptributario,receptor,\
                         sum(iva_fact) as iva_fact,\
                         sum(iva_nc) as iva_nc, sum(qty_docto) as qty_docto,sum(iva_neto) as iva_neto \
               from   tmp_res_docto0 as t1\
               where  not exists(select * \
                                from tmp_f29 as t2 \
                                where tipo = 'f29'\
                                and   t2.CONT_RUT = t1.det_rut_doc_e \
                                and   t2.periodo = t1.dcv_ptributario)\
                group by  det_rut_doc_e, dcv_ptributario,receptor")

    df.registerTempTable('tmp_emi_sinf29')
    #212112: 112.927
    #2021: 1.515.404


    # In[37]:


    #PASO INTERMEDIO: CONTRIBUYENTES QUE EMITEN DOCUMENTOS TIPO 30 EN EL PERIODO Y NO DEBIESEN COMO FUENTE DE CONTAMINACION

    # Documentos de RCV 33
    df_aux=spark.sql(f"select det_rut_doc_e, dcv_ptributario,det_rut_doc_e from dwbgdata.dcv_generic_det_consolidado_sas\
               where   det_tipo_doc IN (30)\
               and   dcv_ptributario={Periodo}\
               and   dcv_operacion = 'COMPRA'")

    # Ejecutar la segunda consulta para obtener los RUT que queremos excluir
    df_excluir = spark.sql("""
        SELECT DISTINCT cont_rut
        FROM dw.dw_trn_riac_atributo_contrib_e
        WHERE tatr_codigo = 'POFE' AND atrc_fecha_termino IS NULL
    """)

    # Hacer un join anti para excluir los RUT que están en df_excluir
    df_resultado = df_aux.join(df_excluir, df_aux.det_rut_doc_e== df_excluir.cont_rut, "left_anti")
    df_resultado.show()               
    df_resultado.registerTempTable('tmp_doc30') 

    # Seleccionar solo los valores únicos de dhdr_rut_emisor
    df_rut_unicos = df_resultado.select("det_rut_doc_e").distinct()

    # Contar el número de valores únicos de dhdr_rut_emisor
    count_rut_unicos = df_rut_unicos.count()
    count_rut_unicos


    # In[38]:


    #Paso 4
    #Contribuyentes  que presentan Observacione W08
    df=spark.sql(f" select cont_rut,PERI_AGNO_MES,INF3_OBSC_DISCRIMINANTE \
                               from DW.DW_TRN_IVAPRO_INF3_VIG_e as t2 \
                               where t2.peri_agno_mes in ({Periodo}) \
                               and t2.inf3_obsr_codigo = 'W08'\
                               and INF3_FECHA_FIN_OBS_DW is null\
                               and  INF3_ESTADO_VIGENCIA='V'")
    df.registerTempTable('tmp_obsW08') 
    #202112:


    # In[39]:


    #Paso 5
    #Contribuyentes que presentan Alertas
    df=spark.sql("select distinct t1.det_rut_doc_e, 'ALERTA' as inf3_obsr_codigo \
                 from tmp_res_docto0 as t1 \
                 where exists(SELECT *\
                              FROM DW.DW_TRN_ALERTAS_E as t2\
                              WHERE t1.det_rut_doc_e = t2.cont_rut\
                              AND   t2.aler_usuario_desactiv_vo is null \
                              AND t2.aler_folio_desbloq_vo IS NULL \
                              AND t2.aler_cod_tipo_alerta_vo IN ('4110','4111','4112'))")

    df.registerTempTable('tmp_alerta') 
    print("tmp_alerta ok")
    #2.276


    # In[40]:


    #Paso 6
    #
    df=spark.sql(f"select cont_rut, monto_cr_especial, left(periodo,6) as periodo\
              from tmp_f29\
              where 1=1\
              and   left(periodo,6) in ({Periodo})\
              and  monto_cr_especial > 0")

    df.registerTempTable('tmp_credito_especial')
    #408.091


    # In[41]:


    #Agrega en tmp_res_docto nodos fantasmas de:
    #   Crédito Especial
    #   W08
    #   Remanente
    df=spark.sql("select *,'NORMAL' as tipo_nodo \
               from tmp_res_docto0\
               union\
               select cont_rut, 0,0,cont_rut,periodo,0,0,0,sum(monto_cr_especial),0,cont_rut,1,1,'FANTASMA CE'\
               from tmp_credito_especial\
               group by cont_rut, periodo\
               union\
               select cont_rut, 0,0,cont_rut,PERI_AGNO_MES,0,0,0,sum(INF3_OBSC_DISCRIMINANTE),0,cont_rut,1,1,'FANTASMA W08'\
               from tmp_obsW08\
               group by cont_rut, PERI_AGNO_MES\
               union\
               select receptor, 0,0,receptor,periodo,0,0,0,prop_remanente_cont,0,receptor,1,1,'FANTASMA REM'\
               from  tmp_remanente_ant\
               union\
               select cont_rut, 0,0,cont_rut,PERI_AGNO_MES,0,0,0,sum(INF3_OBSC_DISCRIMINANTE),0,cont_rut,1,1,'FANTASMA FM01'\
               from tmp_obsfm01_1\
               group by cont_rut, PERI_AGNO_MES")

    #df.registerTempTable("tmp_res_docto")

    #df.show()

    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/res_docto_updated")

    #15.095.576


    #FANTASMA|   408.088
    #  NORMAL|14.687.488


    # In[42]:


    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/res_docto_updated")\
         .registerTempTable('tmp_res_docto')
    print("tmp_res_docto ok")


    # In[43]:


    #spark.sql("select count(*) from tmp_res_docto where tipo_nodo = 'FANTASMA FM01'").show()
    #spark.sql("select count(*) from tmp_obsfm01").show()


    # In[44]:


    # Paso 5
    # Tabla de Relación 
    # Nivel 0
    df=spark.sql("select distinct det_rut_doc_e, receptor,\
                     iva_fact, iva_nc,dcv_ptributario, 0 as nivel, qty_docto,iva_neto,tipo_nodo,emis_declara_f29,rece_declara_f29\
               from tmp_res_docto")

    #df.registerTempTable('tmp_relacion0')

    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/relacion0")
    #202112: 15.969.588
        #    15.095.576
    #2021: 173.021.529


    # In[45]:


    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/relacion0").registerTempTable('tmp_relacion0')
    print("tmp_relacion0 ok")


    # In[46]:


    # Paso 5.1
    #DENTRO DEL MISMO PERIODO
    # Contribuyntes (receptores) que han comprado a (emisores) no declarantes de F29 o emisores que tienen observaciobes W01.
    # considera que los receptores tengan declaracion F29 en la parte de Crédito (520, 762,766,525,528)
    # PRIMER ARCO
    # fact_re_sup: 762, fact_re_const: 766, fact_re_act: 525, nc_re: 528


    # Agregar lógica de NETO = 0 cuando emis_declara_f29 = 0 and rece_declara_f29 = 0
    # a pesar de no tener decaración den F29 se corta la conectividad sólo cuando hay 2 no declarantes 

    # ORIGEN:
    #1: NO DECLARA
    #2: ALERTA
    #3: CR ESPECIAL
    #4: OBSERVACION W08
    #5: IVA REMANENTE
    #6: OBSERVACION FM01
    #7: DECLARANTE EMISOR DE DOCUMENTO TIPO 30 EN PERIODO
    df = spark.sql("select distinct t1.det_rut_doc_e, receptor, \
                       t1.dcv_ptributario, \
                      case when iva_fact <= monto_fact_re then iva_fact else monto_fact_re end as iva_fact_contaminado, \
                      case when iva_nc <= nc_re then iva_nc else nc_re end as iva_nc_contaminado, iva_fact, iva_nc, nc_re, iva_neto, \
                      t1.qty_docto, monto_fact_re, t5.monto_neto_re, \
                      case when iva_fact <= monto_fact_re then iva_fact else monto_fact_re end - \
                      case when iva_nc <= nc_re then iva_nc else nc_re end as neto_ctdo, \
                      case when emis_declara_f29 = 0 and rece_declara_f29 = 0 then 0 \
                           when iva_neto >= 0 and iva_neto <= t5.monto_neto_re then iva_neto \
                           else t5.monto_neto_re end as iva_neto_contaminado, \
                      7 as origen, t5.tipo, emis_declara_f29, rece_declara_f29 \
               from tmp_relacion0 as t1 \
               inner join tmp_f29 as t5 on t1.receptor = t5.cont_rut and t1.dcv_ptributario = t5.periodo and tipo = 'f29' and (t5.F29_c_520 > 0 \
                             or t5.fact_re_sup > 0 \
                             or t5.fact_re_const > 0 \
                             or t5.fact_re_act > 0 \
                             or t5.nc_re > 0) \
               where (exists(select * \
                           from tmp_doc30 as t2 \
                            where t1.det_rut_doc_e = t2.det_rut_doc_e \
                            and   t1.dcv_ptributario=t2.dcv_ptributario) ) \
                union \
                select distinct t1.det_rut_doc_e, receptor, \
                       t1.dcv_ptributario, \
                      case when iva_fact <= monto_fact_re then iva_fact else monto_fact_re end as iva_fact_contaminado, \
                      case when iva_nc <= nc_re then iva_nc else nc_re end as iva_nc_contaminado, iva_fact, iva_nc, nc_re, iva_neto, \
                      t1.qty_docto, monto_fact_re, t5.monto_neto_re, \
                      case when iva_fact <= monto_fact_re then iva_fact else monto_fact_re end - \
                      case when iva_nc <= nc_re then iva_nc else nc_re end as neto_ctdo, \
                      case when emis_declara_f29 = 0 and rece_declara_f29 = 0 then 0 \
                           when iva_neto >= 0 and iva_neto <= t5.monto_neto_re then iva_neto \
                           else t5.monto_neto_re end as iva_neto_contaminado, \
                      1 as origen, t5.tipo, emis_declara_f29, rece_declara_f29 \
               from tmp_relacion0 as t1 \
               inner join tmp_f29 as t5 on t1.receptor = t5.cont_rut and t1.dcv_ptributario = t5.periodo and tipo = 'f29' and (t5.F29_c_520 > 0 \
                             or t5.fact_re_sup > 0 \
                             or t5.fact_re_const > 0 \
                             or t5.fact_re_act > 0 \
                             or t5.nc_re > 0) \
               where (exists(select * \
                           from tmp_emi_sinf29 as t2 \
                            where t1.det_rut_doc_e = t2.det_rut_doc_e \
                            and   t1.dcv_ptributario=t2.dcv_ptributario) ) \
                union \
                select distinct t1.det_rut_doc_e, receptor, \
                       t1.dcv_ptributario, \
                      case when iva_fact <= monto_fact_re then iva_fact else monto_fact_re end as iva_fact_contaminado, \
                      case when iva_nc <= nc_re then iva_nc else nc_re end as iva_nc_contaminado, iva_fact, iva_nc, nc_re, iva_neto, \
                      t1.qty_docto, monto_fact_re, t5.monto_neto_re, \
                      case when iva_fact <= monto_fact_re then iva_fact else monto_fact_re end - \
                      case when iva_nc <= nc_re then iva_nc else nc_re end as neto_ctdo, \
                      case when iva_neto >= 0 and iva_neto <= t5.monto_neto_re then iva_neto \
                           else t5.monto_neto_re end as iva_neto_contaminado, \
                      2 as origen, t5.tipo, emis_declara_f29, rece_declara_f29 \
               from tmp_relacion0 as t1 \
               inner join tmp_f29 as t5 on t1.receptor = t5.cont_rut and t1.dcv_ptributario = t5.periodo and tipo = 'f29' and (t5.F29_c_520 > 0 \
                             or t5.fact_re_sup > 0 \
                             or t5.fact_re_const > 0 \
                             or t5.fact_re_act > 0 \
                             or t5.nc_re > 0) \
               where (exists(select * \
                           from tmp_alerta as ta \
                            where t1.det_rut_doc_e = ta.det_rut_doc_e) ) \
                union \
                select distinct t1.det_rut_doc_e, receptor, \
                       t1.dcv_ptributario, \
                      case when iva_fact <= monto_fact_re then iva_fact else monto_fact_re end as iva_fact_contaminado, \
                      case when iva_nc <= nc_re then iva_nc else nc_re end as iva_nc_contaminado, iva_fact, iva_nc, nc_re, iva_neto, \
                      t1.qty_docto, monto_fact_re, t5.monto_neto_re, \
                      case when iva_fact <= monto_fact_re then iva_fact else monto_fact_re end - \
                      case when iva_nc <= nc_re then iva_nc else nc_re end as neto_ctdo, \
                      case when iva_neto >= 0 and iva_neto <= t5.monto_neto_re then iva_neto \
                           else t5.monto_neto_re end as iva_neto_contaminado, \
                      2 as origen, t5.tipo, emis_declara_f29, rece_declara_f29 \
               from tmp_relacion0 as t1 \
               inner join tmp_f29 as t5 on t1.receptor = t5.cont_rut and t1.dcv_ptributario = t5.periodo and tipo = 'rcv' \
               where (exists(select * \
                           from tmp_alerta as ta \
                            where t1.det_rut_doc_e = ta.det_rut_doc_e) ) \
                and not exists(select * \
                             from tmp_f29 as t3 \
                            where t1.receptor = t3.cont_rut \
                           and   t1.dcv_ptributario = t3.periodo \
                           and   t3.tipo = 'f29' \
                           and (t3.F29_c_520 > 0 \
                             or t3.fact_re_sup > 0 \
                             or t3.fact_re_const > 0 \
                             or t3.fact_re_act > 0 \
                             or t3.nc_re > 0)) \
                union \
                select distinct t1.det_rut_doc_e, receptor, \
                       t1.dcv_ptributario, \
                      case when iva_fact <= monto_fact_re then iva_fact else monto_fact_re end as iva_fact_contaminado, \
                      case when iva_nc <= nc_re then iva_nc else nc_re end as iva_nc_contaminado, iva_fact, iva_nc, nc_re, iva_neto, \
                      t1.qty_docto, monto_fact_re, t5.monto_neto_re, \
                      case when iva_fact <= monto_fact_re then iva_fact else monto_fact_re end - \
                      case when iva_nc <= nc_re then iva_nc else nc_re end as neto_ctdo, \
                      case when iva_neto >= 0 and iva_neto <= t5.monto_neto_re + t5.monto_cr_especial then iva_neto \
                           else t5.monto_neto_re + t5.monto_cr_especial end as iva_neto_contaminado, \
                      3 as origen, 'f29 CE', emis_declara_f29, rece_declara_f29 \
               from tmp_relacion0 as t1 \
               inner join tmp_f29 as t5 on t1.receptor = t5.cont_rut and t1.dcv_ptributario = t5.periodo and tipo = 'f29' \
               where t1.tipo_nodo = 'FANTASMA CE' \
               union \
               select distinct t1.det_rut_doc_e, receptor, \
                      t1.dcv_ptributario, \
                      case when iva_fact <= monto_fact_re then iva_fact else monto_fact_re end as iva_fact_contaminado, \
                      case when iva_nc <= nc_re then iva_nc else nc_re end as iva_nc_contaminado, iva_fact, iva_nc, nc_re, iva_neto, \
                      t1.qty_docto, monto_fact_re, t5.monto_neto_re, \
                      case when iva_fact <= monto_fact_re then iva_fact else monto_fact_re end - \
                      case when iva_nc <= nc_re then iva_nc else nc_re end as neto_ctdo, \
                      case when iva_neto >= 0 and iva_neto <= t5.monto_neto_re + t5.monto_cr_especial then iva_neto \
                           else t5.monto_neto_re + t5.monto_cr_especial end as iva_neto_contaminado, \
                      4 as origen, 'RCV CE', emis_declara_f29, rece_declara_f29 \
               from tmp_relacion0 as t1 \
               inner join tmp_f29 as t5 on t1.receptor = t5.cont_rut and t1.dcv_ptributario = t5.periodo and tipo = 'rcv' \
               where t1.tipo_nodo = 'FANTASMA CE' \
               ")


               #where (exists(select * \
               #            from tmp_credito_especial as t2 \
               #             where t1.det_rut_doc_e = t2.cont_rut\
               #             and   t1.receptor = t2.cont_rut\
               #             and   t1.dcv_ptributario=t2.periodo) )")
    #            and (exists(select * \
    #                       from tmp_credito_especial as t2 \
    #                        where t1.receptor = t2.cont_rut \
    #                        and   t1.dcv_ptributario=t2.periodo) )")


    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/arco1")

    #df.registerTempTable('tmp_arco1')

    # 202112:  86.669 sin considerar observación W01
    # 154.925
    # 592.433
    #10.185.303 agregando crédito especial. Sólo con crédito epecial son 9.983.302
    #7.209.824

    print("tmp_arco1 ok")


    # In[47]:


    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/arco1").registerTempTable('tmp_arco1')


    # In[48]:


    # Paso 6
    # Resultado 1er Arco
    # Reune a todos los receprores del 1 arco que declararon compras en F29, totaliza lo comprado
    # para contrastarlo respecto de lo total declarado
    df=spark.sql("select t1.dcv_ptributario, receptor, \
                      case when sum(iva_fact_contaminado) <= max(t2.monto_fact_re) \
                           then sum(iva_fact_contaminado) \
                           else max(t2.monto_fact_re) end as iva_fact_cdo,\
                      max(t2.monto_fact_re) - case when sum(iva_fact_contaminado) <= max(t2.monto_fact_re) \
                                                   then sum(iva_fact_contaminado) \
                                                   else max(t2.monto_fact_re) end as remanente_iva_fact_re, \
                      case when sum(iva_nc_contaminado) <= max(t2.nc_re) \
                           then sum(iva_nc_contaminado) \
                           else max(t2.nc_re) end as iva_nc_cdo,\
                      max(t2.nc_re) - case when sum(iva_nc_contaminado) <= max(t2.nc_re) \
                           then sum(iva_nc_contaminado) \
                           else max(t2.nc_re) end as remanente_iva_nc_re, \
                       case when origen = 3 then sum(iva_neto_contaminado)\
                            when origen != 3 and sum(emis_declara_f29) = 0 and sum(rece_declara_f29) = 0 then 0 \
                            when origen != 3 and sum(iva_neto_contaminado) <= max(t2.monto_neto_re) then sum(iva_neto_contaminado) \
                            else max(t2.monto_neto_re) end as iva_neto_cdo,\
                      sum(qty_docto) as qty_docto,\
                      max(t2.monto_fact) as monto_fact_emi,\
                      max(t2.qty_fact) as qty_fact_emi,\
                      max(t2.monto_nc) as monto_nc_emi,\
                      max(t2.qty_nc) as qty_nc_emi,\
                      max(t2.F29_c_537) as c_537,\
                      max(t2.F29_c_520) as c_520,\
                      max(t2.monto_fact_re) as monto_fact_reci, \
                      max(t2.nc_re) as monto_nc_reci,\
                      max(t2.monto_neto_re)+max(t2.monto_cr_especial) as monto_neto_reci,\
                      t1.origen\
               from tmp_arco1 as t1 \
               inner join tmp_f29 as t2 on t1.receptor = t2.cont_rut and t1.dcv_ptributario = t2.periodo \
               where 1=1\
               group by  t1.dcv_ptributario,receptor, t1.origen")

    df.registerTempTable('tmp_prepara_resultado')
    #202112: 71.745
    #2021: 4.180.038

    #934.973


    # In[49]:


    # Paso 7. Preparación de Archivo
    # Resultado Proporcional Primer Arco
    # Consultar si los montos de NC se restan de lo FACTURADO

    df=spark.sql("select dcv_ptributario,  receptor,\
                       monto_fact_reci as f29_fact_reci, monto_nc_reci as f29_nc_reci,\
                       monto_neto_reci as f29_neto_reci,\
                       case when iva_fact_cdo <= monto_fact_reci then iva_fact_cdo else monto_fact_reci end as iva_fact_cdo, \
                       case when iva_nc_cdo <= monto_nc_reci then iva_nc_cdo else monto_nc_reci end as iva_nc_cdo, \
                      round((coalesce(iva_fact_cdo,0)) / (coalesce(monto_fact_reci,0)),5) as porc_fact_ctdo_r, \
                      round((coalesce(iva_nc_cdo,0) ) / (coalesce(monto_nc_reci,0) ),5) as porc_nc_ctdo_r,c_537,1 as arco,\
                      round((coalesce(iva_neto_cdo,0) ) / (coalesce(monto_neto_reci,0) ),5) as porc_neto_ctdo_r,\
                       case when origen = 3 then iva_neto_cdo \
                            when origen != 3 and iva_neto_cdo <= monto_neto_reci then iva_neto_cdo else monto_neto_reci end as iva_neto_cdo,\
                       origen\
              from  tmp_prepara_resultado")

    #df.registerTempTable('tmp_resultado_arco1')
    #202112 sin obs: 15.912
    #71.745

    #82.253 con  alerta

    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/resultado_arco1")


    # In[50]:


    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/resultado_arco1").registerTempTable('tmp_resultado_arco1')


    # In[51]:


    # Paso 8
    # Determinación del Arco2

    df=spark.sql("select distinct t1.dcv_ptributario, t1.det_rut_doc_e, t1.iva_fact as iva_fact_emisor, t1.iva_nc as iva_nc_emisor,\
                         t1.receptor, \
                         t2.iva_fact_cdo, t2.iva_nc_cdo,t2.iva_neto_cdo,\
                         round(t2.porc_fact_ctdo_r,5) as porc_fact_ctdo_r, \
                         round(t2.porc_nc_ctdo_r,5) as porc_nc_ctdo_r,\
                         round(iva_fact*t2.porc_fact_ctdo_r,1) as pro_iva_fact_contdo,\
                         round(iva_nc*t2.porc_fact_ctdo_r,1) as pro_iva_nc_contdo,\
                         round(t2.porc_neto_ctdo_r,5) as porc_neto_ctdo_r,\
                         round(iva_neto*t2.porc_neto_ctdo_r,1) as pro_iva_neto_contdo, \
                          t2.origen\
                  from tmp_res_docto as t1 \
                  inner join tmp_resultado_arco1 as t2 on t1.det_rut_doc_e = t2.receptor and t1.dcv_ptributario = t2.dcv_ptributario\
                  where 0= 0\
                  and   t1.det_rut_doc_e != t1.receptor")

    df.registerTempTable('tmp_arco2')
    #202112: 8.501.900
    #202112: 14.478.381
    #9.179.867

        #2021: 145.148.942



    # In[52]:


    #Paso 9: Resultado Arco2
    df=spark.sql("select t1.dcv_ptributario, receptor, \
                      max(t2.monto_fact_re) as monto_fact_reci, \
                      max(t2.nc_re) as monto_nc_reci,\
                      max(t2.monto_neto_re)+max(t2.monto_cr_especial) as monto_neto_reci,\
                      case when sum(pro_iva_fact_contdo) <= max(t2.monto_fact_re) then sum(pro_iva_fact_contdo) else max(t2.monto_fact_re) end as iva_fact_cdo, \
                      case when sum(pro_iva_nc_contdo) <= max(t2.nc_re) then sum(pro_iva_nc_contdo) else max(t2.nc_re) end as iva_nc_cdo, \
                      case when sum(pro_iva_neto_contdo) <= max(t2.monto_neto_re) then sum(pro_iva_neto_contdo) else max(t2.monto_neto_re) end as iva_neto_cdo, \
                      case when sum(pro_iva_fact_contdo) <= max(t2.monto_fact_re) \
                           then round(sum(pro_iva_fact_contdo)/max(t2.monto_fact_re),5)\
                      else 1 end as prop_fact_cdo,\
                      case when sum(pro_iva_nc_contdo) <= max(t2.nc_re) \
                           then round(sum(pro_iva_nc_contdo)/max(t2.nc_re),5)\
                      else 1 end as prop_nc_cdo,\
                      case when sum(pro_iva_neto_contdo) <= max(t2.monto_neto_re) \
                           then round(sum(pro_iva_neto_contdo)/max(t2.monto_neto_re),5)\
                      else 1 end as prop_neto_cdo,\
                      max(t2.F29_c_537) as c_537,\
                      2 as arco,\
                      origen\
               from tmp_arco2 as t1 \
               inner join tmp_f29 as t2 on t1.receptor = t2.cont_rut and t1.dcv_ptributario = t2.periodo \
               where 1=1\
               group by t1.dcv_ptributario,receptor,origen")



    #df.registerTempTable('tmp_resultado_arco2')
    #202112: 748.719
    # 202112: 8.498.813

    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/resultado_arco2")


    # In[53]:


    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/resultado_arco2").registerTempTable('tmp_resultado_arco2')
    print("tmp_resultado_arco2 ok")


    # In[54]:


    # Paso 10
    # Determinación del Arco3
    df=spark.sql("select t1.dcv_ptributario, t1.det_rut_doc_e, t1.iva_fact as iva_fact_emisor, t1.iva_nc as iva_nc_emisor,\
                         t1.receptor,\
                         t2.iva_fact_cdo, t2.iva_nc_cdo,t2.iva_neto_cdo,\
                         round(t2.prop_fact_cdo,5) as porc_fact_ctdo_r, \
                         round(t2.prop_nc_cdo,5) as porc_nc_ctdo_r,\
                         round(iva_fact*t2.prop_fact_cdo,1) as pro_iva_fact_contdo,\
                         round(iva_nc*t2.prop_nc_cdo,1) as pro_iva_nc_contdo,\
                         round(iva_neto*t2.prop_neto_cdo,1) as pro_iva_neto_contdo, \
                         t2.origen\
                  from tmp_res_docto as t1 \
                  inner join tmp_resultado_arco2 as t2 on t1.det_rut_doc_e = t2.receptor and t1.dcv_ptributario = t2.dcv_ptributario\
                  where t2.iva_fact_cdo > 0\
                  and   t1.det_rut_doc_e != t1.receptor")

    df.registerTempTable('tmp_arco3')
    #202112: 15.709.746
    #1.131.188


    # In[55]:


    #Paso 11: Resultado Arco3


    df=spark.sql("select t1.dcv_ptributario, receptor,\
                      max(t2.monto_fact_re) as monto_fact_reci,\
                      max(t2.nc_re) as monto_nc_reci,\
                      max(t2.monto_neto_re)+max(t2.monto_cr_especial) as monto_neto_reci,\
                      case when round(sum(pro_iva_fact_contdo),1) <= max(t2.monto_fact_re) then round(sum(pro_iva_fact_contdo),1) else max(t2.monto_fact_re) end as iva_fact_cdo,\
                      case when round(sum(pro_iva_nc_contdo),1) <= max(t2.nc_re) then round(sum(pro_iva_nc_contdo),1) else max(t2.nc_re) end as iva_nc_cdo,\
                      case when round(sum(pro_iva_neto_contdo),1) <= max(t2.monto_neto_re) then round(sum(pro_iva_neto_contdo),1) else max(t2.monto_neto_re) end as iva_neto_cdo,\
                      case when sum(pro_iva_fact_contdo) <= max(t2.monto_fact_re) \
                           then round(sum(pro_iva_fact_contdo)/max(t2.monto_fact_re),5)\
                      else 1 end as prop_fact_cdo,\
                      case when sum(pro_iva_nc_contdo) <= max(t2.nc_re) \
                           then round(sum(pro_iva_nc_contdo)/max(t2.nc_re),5)\
                      else 1 end as prop_nc_cdo,\
                      case when sum(pro_iva_neto_contdo) <= max(t2.monto_neto_re) \
                           then round(sum(pro_iva_neto_contdo)/max(t2.monto_neto_re),5)\
                      else 1 end as prop_neto_cdo,\
                      max(t2.F29_c_537) as c_537,\
                      3 as arco,\
                      origen\
               from tmp_arco3 as t1 \
               inner join tmp_f29 as t2 on t1.receptor = t2.cont_rut and t1.dcv_ptributario = t2.periodo \
               where 1=1\
               group by t1.dcv_ptributario,receptor,origen")

    #df.registerTempTable('tmp_resultado_arco3')
    #202112: 817.023
    #336.809
    #816.291

    #   origen|count(1)|
    #+----------+--------+
    #|    ALERTA|  810485|
    #|NO DECLARA|  816291|

    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/resultado_arco3")


    # In[56]:


    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/resultado_arco3").registerTempTable('tmp_resultado_arco3')
    print("tmp_resultado_arco3 ok")


    # In[57]:


    # Paso 12
    # Determinación del Arco4
    df=spark.sql("select t1.dcv_ptributario, t1.det_rut_doc_e, iva_fact as iva_fact_emisor, iva_nc as iva_nc_emisor, \
                         t1.receptor,  \
                         t2.iva_fact_cdo, t2.iva_nc_cdo,t2.iva_neto_cdo,\
                         round(t2.prop_fact_cdo,1) as porc_fact_ctdo_r, \
                         round(t2.prop_nc_cdo,1) as porc_nc_ctdo_r,\
                         round(iva_fact*t2.prop_fact_cdo,5) as pro_iva_fact_contdo,\
                         round(iva_nc*t2.prop_nc_cdo,5) as pro_iva_nc_contdo,\
                         round(iva_neto*t2.prop_neto_cdo,5) as pro_iva_neto_contdo, \
                         t2.origen\
                  from tmp_res_docto as t1 \
                  inner join tmp_resultado_arco3 as t2 on t1.det_rut_doc_e = t2.receptor and t1.dcv_ptributario = t2.dcv_ptributario\
                  where t2.iva_fact_cdo > 0\
                  and   t1.det_rut_doc_e != t1.receptor")

    df.registerTempTable('tmp_arco4')
    #15.719.882
    #9.185


    # In[58]:


    #Paso 13: Resultado Arco4


    df=spark.sql("select t1.dcv_ptributario, receptor, \
                      max(t2.monto_fact_re) as monto_fact_reci, \
                      max(t2.nc_re) as monto_nc_reci,\
                      max(t2.monto_neto_re)+max(t2.monto_cr_especial) as monto_neto_reci,\
                      case when round(sum(pro_iva_fact_contdo),1) <= max(t2.monto_fact_re) then round(sum(pro_iva_fact_contdo),1) else max(t2.monto_fact_re) end as iva_fact_cdo, \
                      case when round(sum(pro_iva_nc_contdo),1) <= max(t2.nc_re) then round(sum(pro_iva_nc_contdo),1) else max(t2.nc_re) end as iva_nc_cdo, \
                      case when round(sum(pro_iva_neto_contdo),1) <= max(t2.monto_neto_re) then round(sum(pro_iva_neto_contdo),1) else max(t2.monto_neto_re) end as iva_neto_cdo, \
                      case when sum(pro_iva_fact_contdo) <= max(t2.monto_fact_re) \
                           then round(sum(pro_iva_fact_contdo)/max(t2.monto_fact_re),5)\
                      else 1 end as prop_fact_cdo,\
                      case when sum(pro_iva_nc_contdo) <= max(t2.nc_re) \
                           then round(sum(pro_iva_nc_contdo)/max(t2.nc_re),5)\
                      else 1 end as prop_nc_cdo,\
                      case when sum(pro_iva_neto_contdo) <= max(t2.monto_neto_re) \
                           then round(sum(pro_iva_neto_contdo)/max(t2.monto_neto_re),5)\
                      else 1 end as prop_neto_cdo,\
                      max(t2.F29_c_537) as c_537,\
                      4 as arco,\
                      origen\
               from tmp_arco4 as t1 \
               inner join tmp_f29 as t2 on t1.receptor = t2.cont_rut and t1.dcv_ptributario = t2.periodo \
               where 1=1\
               group by t1.dcv_ptributario,receptor,origen")

    #df.registerTempTable('tmp_resultado_arco4')
    #202112: 817.142
    #6.928

    #+----------+--------+
    #|    origen|count(1)|
    #+----------+--------+
    #|    ALERTA|  816993|
    #|NO DECLARA|  817109|
    #+----------+--------+

    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/resultado_arco4")


    # In[59]:


    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/resultado_arco4").registerTempTable('tmp_resultado_arco4')
    print("tmp_resultado_arco4 ok")


    # In[60]:


    # Paso 14
    # Determinación del Arco5
    df=spark.sql("select t1.dcv_ptributario, t1.det_rut_doc_e, iva_fact as iva_fact_emisor, iva_nc as iva_nc_emisor, \
                         t1.receptor,  \
                         t2.iva_fact_cdo, t2.iva_nc_cdo,t2.iva_neto_cdo,\
                         round(t2.prop_fact_cdo,1) as porc_fact_ctdo_r, \
                         round(t2.prop_nc_cdo,1) as porc_nc_ctdo_r,\
                         round(iva_fact*t2.prop_fact_cdo,5) as pro_iva_fact_contdo,\
                         round(iva_nc*t2.prop_nc_cdo,5) as pro_iva_nc_contdo,\
                         round(iva_neto*t2.prop_neto_cdo,5) as pro_iva_neto_contdo, \
                         t2.origen\
                  from tmp_res_docto as t1 \
                  inner join tmp_resultado_arco4 as t2 on t1.det_rut_doc_e = t2.receptor and t1.dcv_ptributario = t2.dcv_ptributario\
                  where t2.iva_fact_cdo > 0\
                  and   t1.det_rut_doc_e != t1.receptor")

    df.registerTempTable('tmp_arco5')
    #202112: 15.719.925
    #9.185


    # In[61]:


    ####################################################
    ########Paso 15: Resultado Arco5 ###################
    ####################################################

    df=spark.sql("select t1.dcv_ptributario, receptor, \
                      max(t2.monto_fact_re) as monto_fact_reci, \
                      max(t2.nc_re) as monto_nc_reci,\
                      max(t2.monto_neto_re)+max(t2.monto_cr_especial) as monto_neto_reci,\
                      case when round(sum(pro_iva_fact_contdo),1) <= max(t2.monto_fact_re) then round(sum(pro_iva_fact_contdo),1) else max(t2.monto_fact_re) end as iva_fact_cdo, \
                      case when round(sum(pro_iva_nc_contdo),1) <= max(t2.nc_re) then round(sum(pro_iva_nc_contdo),1) else max(t2.nc_re) end as iva_nc_cdo, \
                      case when round(sum(pro_iva_neto_contdo),1) <= max(t2.monto_neto_re) then round(sum(pro_iva_neto_contdo),1) else max(t2.monto_neto_re) end as iva_neto_cdo, \
                      case when sum(pro_iva_fact_contdo) <= max(t2.monto_fact_re) \
                           then round(sum(pro_iva_fact_contdo)/max(t2.monto_fact_re),5)\
                      else 1 end as prop_fact_cdo,\
                      case when sum(pro_iva_nc_contdo) <= max(t2.nc_re) \
                           then round(sum(pro_iva_nc_contdo)/max(t2.nc_re),5)\
                      else 1 end as prop_nc_cdo,\
                      case when sum(pro_iva_neto_contdo) <= max(t2.monto_neto_re) \
                           then round(sum(pro_iva_neto_contdo)/max(t2.monto_neto_re),5)\
                      else 1 end as prop_neto_cdo,\
                      max(t2.F29_c_537) as c_537,\
                      5 as arco,\
                      t1.origen\
               from tmp_arco5 as t1 \
               inner join tmp_f29 as t2 on t1.receptor = t2.cont_rut and t1.dcv_ptributario = t2.periodo \
               where 1=1\
               group by t1.dcv_ptributario,receptor,origen")

    #df.registerTempTable('tmp_resultado_arco5')
    #817.149
    #109

    #+----------+--------+
    #|    origen|count(1)|
    #+----------+--------+
    #|    ALERTA|  817153|
    #|NO DECLARA|  817131|
    #+----------+--------+

    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/resultado_arco5")


    # In[62]:


    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/resultado_arco5").registerTempTable('tmp_resultado_arco5')
    print("tmp_resultado_arco5 ok")


    # In[63]:


    #Para exportar los datos en archivo plano
    df=spark.sql("select dcv_ptributario,receptor,f29_fact_reci,f29_nc_reci,f29_neto_reci, iva_fact_cdo, iva_nc_cdo,iva_neto_cdo,\
                         porc_fact_ctdo_r, porc_nc_ctdo_r, porc_neto_ctdo_r,arco, c_537,origen\
                  from tmp_resultado_arco1\
                  union\
                  select dcv_ptributario,receptor,monto_fact_reci,monto_nc_reci,monto_neto_reci,iva_fact_cdo,iva_nc_cdo,iva_neto_cdo,\
                         prop_fact_cdo,prop_nc_cdo,prop_neto_cdo,arco,c_537,origen\
                  from tmp_resultado_arco2\
                  union \
                  select dcv_ptributario,receptor,monto_fact_reci,monto_nc_reci,monto_neto_reci,iva_fact_cdo,iva_nc_cdo,iva_neto_cdo,\
                         prop_fact_cdo,prop_nc_cdo,prop_neto_cdo,arco,c_537,origen\
                  from tmp_resultado_arco3\
                  union \
                  select dcv_ptributario,receptor,monto_fact_reci,monto_nc_reci,monto_neto_reci,iva_fact_cdo,iva_nc_cdo,iva_neto_cdo,\
                         prop_fact_cdo,prop_nc_cdo,prop_neto_cdo,arco,c_537,origen\
                  from tmp_resultado_arco4\
                  union \
                  select dcv_ptributario,receptor,monto_fact_reci,monto_nc_reci,monto_neto_reci,iva_fact_cdo,iva_nc_cdo,iva_neto_cdo,\
                         prop_fact_cdo,prop_nc_cdo,prop_neto_cdo,arco,c_537,origen\
                  from tmp_resultado_arco5")

    #df.registerTempTable("tmp_resultado")
    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/resultado")
    #3.649.767 


    # In[64]:


    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/resultado").registerTempTable('tmp_resultado')

    print("tmp_resultado ok")


    # In[65]:


    #Paso 16: Totaliza resultado de Receptores
    df=spark.sql("select t1.dcv_ptributario, t1.receptor, max(t1.f29_fact_reci) as f29_fact_reci, max(t1.f29_nc_reci) as f29_nc_reci, \
                         max(t1.f29_neto_reci) as f29_neto_reci,\
                         0 as iva_fact_cdo, \
                         0 as iva_nc_cdo, \
                         0 as iva_neto_cdo,\
                         0 as prop_fact_cdo, \
                         0 as prop_nc_cdo, \
                         0 as prop_neto_cdo, \
                         max(t1.c_537) as c_537,\
                         count(*) as qty_arco,origen\
                  from tmp_resultado as t1\
                  inner join tmp_f29_1 as t2 on t1.receptor = t2.cont_rut and t1.dcv_ptributario = t2.periodo\
                  group by dcv_ptributario, receptor,origen")

    #df.registerTempTable('tmp_prep_resultado_resumen')

    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/prep_resultado_resumen")


    # In[66]:


    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/prep_resultado_resumen").registerTempTable('tmp_prep_resultado_resumen')

    print("tmp_prep_resultado_resumen ok")


    # In[67]:


    df=spark.sql("select t1.dcv_ptributario, t1.receptor, t1.f29_fact_reci, t1.f29_nc_reci, \
                      t1.f29_neto_reci,t1.c_537,qty_arco,\
                      case \
                          when t2.origen = -3 then coalesce(t2.iva_neto_cdo,0) \
                          when coalesce(t2.iva_neto_cdo,0) > 0 and coalesce(t2.iva_neto_cdo,0) > t1.f29_neto_reci then t1.f29_neto_reci \
                          when coalesce(t2.iva_neto_cdo,0) < 0 then coalesce(t2.iva_neto_cdo,0) \
                          else coalesce(t2.iva_neto_cdo,0) \
                      end as iva_neto_cdo_a1,\
                      case \
                          when coalesce(t2.porc_neto_ctdo_r,0) > 1 then 1 \
                          when coalesce(t2.porc_neto_ctdo_r,0) < 0 then coalesce(t2.porc_neto_ctdo_r,0) \
                          else coalesce(t2.porc_neto_ctdo_r,0) \
                      end as prop_neto_cdo_a1,\
                      case \
                          when t3.origen = -3 then coalesce(t3.iva_neto_cdo,0) \
                          when coalesce(t3.iva_neto_cdo,0) > 0 and coalesce(t3.iva_neto_cdo,0) > t1.f29_neto_reci then t1.f29_neto_reci \
                          when coalesce(t3.iva_neto_cdo,0) < 0 then coalesce(t3.iva_neto_cdo,0) \
                          else coalesce(t3.iva_neto_cdo,0) \
                      end as  iva_neto_cdo_a2,\
                      case \
                          when coalesce(t3.prop_neto_cdo,0) > 1 then 1 \
                          when coalesce(t3.prop_neto_cdo,0) < 0 then coalesce(t3.prop_neto_cdo,0) \
                          else coalesce(t3.prop_neto_cdo,0) \
                      end as prop_neto_cdo_a2,\
                      case \
                          when t4.origen = -3 then coalesce(t4.iva_neto_cdo,0) \
                          when coalesce(t4.iva_neto_cdo,0) > 0 and coalesce(t4.iva_neto_cdo,0) > t1.f29_neto_reci then t1.f29_neto_reci \
                          when coalesce(t4.iva_neto_cdo,0) < 0 then coalesce(t4.iva_neto_cdo,0) \
                          else coalesce(t4.iva_neto_cdo,0) \
                      end as  iva_neto_cdo_a3,\
                      case \
                          when coalesce(t4.prop_neto_cdo,0) > 1 then 1 \
                          when coalesce(t4.prop_neto_cdo,0) < 0 then coalesce(t4.prop_neto_cdo,0) \
                          else coalesce(t4.prop_neto_cdo,0) \
                      end as prop_neto_cdo_a3,\
                      case \
                          when t5.origen = -3 then coalesce(t5.iva_neto_cdo,0) \
                          when coalesce(t5.iva_neto_cdo,0) > 0 and coalesce(t5.iva_neto_cdo,0) > t1.f29_neto_reci then t1.f29_neto_reci \
                          when coalesce(t5.iva_neto_cdo,0) < 0 then coalesce(t5.iva_neto_cdo,0) \
                          else coalesce(t5.iva_neto_cdo,0) \
                      end as  iva_neto_cdo_a4,\
                      case \
                          when coalesce(t5.prop_neto_cdo,0) > 1 then 1 \
                          when coalesce(t5.prop_neto_cdo,0) < 0 then coalesce(t5.prop_neto_cdo,0) \
                          else coalesce(t5.prop_neto_cdo,0) \
                      end as prop_neto_cdo_a4,\
                      case \
                          when t6.origen = -3 then coalesce(t6.iva_neto_cdo,0) \
                          when coalesce(t6.iva_neto_cdo,0) > 0 and coalesce(t6.iva_neto_cdo,0) > t1.f29_neto_reci then t1.f29_neto_reci \
                          when coalesce(t6.iva_neto_cdo,0) < 0 then coalesce(t6.iva_neto_cdo,0) \
                          else coalesce(t6.iva_neto_cdo,0) \
                      end as  iva_neto_cdo_a5,\
                      case \
                          when coalesce(t6.prop_neto_cdo,0) > 1 then 1 \
                          when coalesce(t6.prop_neto_cdo,0) < 0 then coalesce(t6.prop_neto_cdo,0) \
                          else coalesce(t6.prop_neto_cdo,0) \
                      end as prop_neto_cdo_a5,\
                      case \
                          when t2.origen = 3 then coalesce(t2.iva_neto_cdo,0) \
                          when coalesce(t2.iva_neto_cdo,0) > 0 and coalesce(t2.iva_neto_cdo,0) > t1.f29_neto_reci then t1.f29_neto_reci \
                          when coalesce(t2.iva_neto_cdo,0) < 0 then coalesce(t2.iva_neto_cdo,0) \
                          else coalesce(t2.iva_neto_cdo,0) \
                      end +\
                      case \
                          when t3.origen = -3 then coalesce(t3.iva_neto_cdo,0) \
                          when coalesce(t3.iva_neto_cdo,0) > 0 and coalesce(t3.iva_neto_cdo,0) > t1.f29_neto_reci then t1.f29_neto_reci \
                          when coalesce(t3.iva_neto_cdo,0) < 0 then coalesce(t3.iva_neto_cdo,0) \
                          else coalesce(t3.iva_neto_cdo,0) \
                      end +\
                      case \
                          when t4.origen = -3 then coalesce(t4.iva_neto_cdo,0) \
                          when coalesce(t4.iva_neto_cdo,0) > 0 and coalesce(t4.iva_neto_cdo,0) > t1.f29_neto_reci then t1.f29_neto_reci \
                          when coalesce(t4.iva_neto_cdo,0) < 0 then coalesce(t4.iva_neto_cdo,0) \
                          else coalesce(t4.iva_neto_cdo,0) \
                      end +\
                      case \
                          when t5.origen = -3 then coalesce(t5.iva_neto_cdo,0) \
                          when coalesce(t5.iva_neto_cdo,0) > 0 and coalesce(t5.iva_neto_cdo,0) > t1.f29_neto_reci then t1.f29_neto_reci \
                          when coalesce(t5.iva_neto_cdo,0) < 0 then coalesce(t5.iva_neto_cdo,0) \
                          else coalesce(t5.iva_neto_cdo,0) \
                      end +\
                      case \
                          when t6.origen = -3 then coalesce(t6.iva_neto_cdo,0) \
                          when coalesce(t6.iva_neto_cdo,0) > 0 and coalesce(t6.iva_neto_cdo,0) > t1.f29_neto_reci then t1.f29_neto_reci \
                          when coalesce(t6.iva_neto_cdo,0) < 0 then coalesce(t6.iva_neto_cdo,0) \
                          else coalesce(t6.iva_neto_cdo,0) \
                      end as  total_cont,\
                case when \
                      case \
                          when t2.origen = -3 then coalesce(t2.iva_neto_cdo,0) \
                          when coalesce(t2.iva_neto_cdo,0) > 0 and coalesce(t2.iva_neto_cdo,0) > t1.f29_neto_reci then t1.f29_neto_reci \
                          when coalesce(t2.iva_neto_cdo,0) < 0 then coalesce(t2.iva_neto_cdo,0) \
                          else coalesce(t2.iva_neto_cdo,0) \
                      end +\
                      case \
                          when t3.origen = -3 then coalesce(t3.iva_neto_cdo,0) \
                          when coalesce(t3.iva_neto_cdo,0) > 0 and coalesce(t3.iva_neto_cdo,0) > t1.f29_neto_reci then t1.f29_neto_reci \
                          when coalesce(t3.iva_neto_cdo,0) < 0 then coalesce(t3.iva_neto_cdo,0) \
                          else coalesce(t3.iva_neto_cdo,0) \
                      end +\
                      case \
                          when t4.origen = -3 then coalesce(t4.iva_neto_cdo,0) \
                          when coalesce(t4.iva_neto_cdo,0) > 0 and coalesce(t4.iva_neto_cdo,0) > t1.f29_neto_reci then t1.f29_neto_reci \
                          when coalesce(t4.iva_neto_cdo,0) < 0 then coalesce(t4.iva_neto_cdo,0) \
                          else coalesce(t4.iva_neto_cdo,0) \
                      end +\
                      case \
                          when t5.origen = -3 then coalesce(t5.iva_neto_cdo,0) \
                          when coalesce(t5.iva_neto_cdo,0) > 0 and coalesce(t5.iva_neto_cdo,0) > t1.f29_neto_reci then t1.f29_neto_reci \
                          when coalesce(t5.iva_neto_cdo,0) < 0 then coalesce(t5.iva_neto_cdo,0) \
                          else coalesce(t5.iva_neto_cdo,0) \
                      end +\
                      case \
                          when t6.origen = -3 then coalesce(t6.iva_neto_cdo,0) \
                          when coalesce(t6.iva_neto_cdo,0) > 0 and coalesce(t6.iva_neto_cdo,0) > t1.f29_neto_reci then t1.f29_neto_reci \
                          when coalesce(t6.iva_neto_cdo,0) < 0 then coalesce(t6.iva_neto_cdo,0) \
                          else coalesce(t6.iva_neto_cdo,0) \
                      end > t1.f29_neto_reci then 1 \
                    else \
                      (case \
                          when t2.origen = -3 then coalesce(t2.iva_neto_cdo,0) \
                          when coalesce(t2.iva_neto_cdo,0) > 0 and coalesce(t2.iva_neto_cdo,0) > t1.f29_neto_reci then t1.f29_neto_reci \
                          when coalesce(t2.iva_neto_cdo,0) < 0 then coalesce(t2.iva_neto_cdo,0) \
                          else coalesce(t2.iva_neto_cdo,0) \
                      end +\
                      case \
                          when t3.origen = -3 then coalesce(t3.iva_neto_cdo,0) \
                          when coalesce(t3.iva_neto_cdo,0) > 0 and coalesce(t3.iva_neto_cdo,0) > t1.f29_neto_reci then t1.f29_neto_reci \
                          when coalesce(t3.iva_neto_cdo,0) < 0 then coalesce(t3.iva_neto_cdo,0) \
                          else coalesce(t3.iva_neto_cdo,0) \
                      end +\
                      case \
                          when t4.origen = -3 then coalesce(t4.iva_neto_cdo,0) \
                          when coalesce(t4.iva_neto_cdo,0) > 0 and coalesce(t4.iva_neto_cdo,0) > t1.f29_neto_reci then t1.f29_neto_reci \
                          when coalesce(t4.iva_neto_cdo,0) < 0 then coalesce(t4.iva_neto_cdo,0) \
                          else coalesce(t4.iva_neto_cdo,0) \
                      end +\
                      case \
                          when t5.origen = -3 then coalesce(t5.iva_neto_cdo,0) \
                          when coalesce(t5.iva_neto_cdo,0) > 0 and coalesce(t5.iva_neto_cdo,0) > t1.f29_neto_reci then t1.f29_neto_reci \
                          when coalesce(t5.iva_neto_cdo,0) < 0 then coalesce(t5.iva_neto_cdo,0) \
                          else coalesce(t5.iva_neto_cdo,0) \
                      end +\
                      case \
                          when t6.origen = -3 then coalesce(t6.iva_neto_cdo,0) \
                          when coalesce(t6.iva_neto_cdo,0) > 0 and coalesce(t6.iva_neto_cdo,0) > t1.f29_neto_reci then t1.f29_neto_reci \
                          when coalesce(t6.iva_neto_cdo,0) < 0 then coalesce(t6.iva_neto_cdo,0) \
                          else coalesce(t6.iva_neto_cdo,0) \
                      end) / t1.f29_neto_reci \
                end as prop_total_cont,\
                      t2.origen as origen_a1,\
                      t3.origen as origen_a2,\
                      t4.origen as origen_a3,\
                      t5.origen as origen_a4,\
                      t6.origen as origen_a5\
              from tmp_prep_resultado_resumen as t1 \
               left join tmp_resultado_arco1 as t2 on t1.dcv_ptributario = t2.dcv_ptributario and t1.receptor = t2.receptor and t1.origen = t2.origen\
               left join tmp_resultado_arco2 as t3 on t1.dcv_ptributario = t3.dcv_ptributario and t1.receptor = t3.receptor and t1.origen = t3.origen\
               left join tmp_resultado_arco3 as t4 on t1.dcv_ptributario = t4.dcv_ptributario and t1.receptor = t4.receptor and t1.origen = t4.origen\
               left join tmp_resultado_arco4 as t5 on t1.dcv_ptributario = t5.dcv_ptributario and t1.receptor = t5.receptor and t1.origen = t5.origen\
               left join tmp_resultado_arco5 as t6 on t1.dcv_ptributario = t6.dcv_ptributario and t1.receptor = t6.receptor and t1.origen = t6.origen\
               where 1=1")

    #df.registerTempTable('tmp_resultado_resumen')
    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/iva_resul_resumen")
    #df.count()
    #818620
    #1.636.111
    #7.000.724


    # In[68]:


    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/iva_resul_resumen").registerTempTable('tmp_resultado_resumen')

    print("tmp_resultado_resumen ok")


    # In[69]:


    # spark.sql("select * from tmp_resultado_resumen where receptor = 'V/3ZEm3kHIMGUMjVTBqquA==' and origen_a1=3").show()


    # In[70]:


    #Paso : Receptores unicos
    df=spark.sql("select dcv_ptributario, receptor, max(f29_fact_reci) as f29_fact_reci, max(f29_nc_reci) as f29_nc_reci, \
                         max(f29_neto_reci) as f29_neto_reci,max(c_537) as c_537\
                  from tmp_resultado as t1\
                  inner join tmp_f29_1 as t2 on t1.receptor = t2.cont_rut and t1.dcv_ptributario = t2.periodo\
                  group by dcv_ptributario, receptor")

    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/iva_receptores")

    #df.registerTempTable('tmp_receptores')
    #3.508.340
    #789.404
    #790.456


    # In[71]:


    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/iva_receptores").registerTempTable('tmp_receptores')

    print("tmp_receptores ok")


    # In[72]:


    df=spark.sql("select tr.dcv_ptributario, tr.receptor, tr.f29_neto_reci, \
                      tal.iva_neto_cdo_a1 as iva_neto_cdo_a1_al,tal.prop_neto_cdo_a1 as prop_neto_cdo_a1_al,\
                      tal.iva_neto_cdo_a2 as iva_neto_cdo_a2_al,tal.prop_neto_cdo_a2 as prop_neto_cdo_a2_al,\
                      tal.iva_neto_cdo_a3 as iva_neto_cdo_a3_al,tal.prop_neto_cdo_a3 as prop_neto_cdo_a3_al,\
                      tal.iva_neto_cdo_a4 as iva_neto_cdo_a4_al,tal.prop_neto_cdo_a4 as prop_neto_cdo_a4_al,\
                      tal.iva_neto_cdo_a5 as iva_neto_cdo_a5_al,tal.prop_neto_cdo_a5 as prop_neto_cdo_a5_al,\
                      tal.total_cont as total_cont_al,tal.prop_total_cont as prop_total_cont_al,\
                      nd.iva_neto_cdo_a1 as iva_neto_cdo_a1_nd,nd.prop_neto_cdo_a1 as prop_neto_cdo_a1_nd,\
                      nd.iva_neto_cdo_a2 as iva_neto_cdo_a2_nd,nd.prop_neto_cdo_a2 as prop_neto_cdo_a2_nd,\
                      nd.iva_neto_cdo_a3 as iva_neto_cdo_a3_nd,nd.prop_neto_cdo_a3 as prop_neto_cdo_a3_nd,\
                      nd.iva_neto_cdo_a4 as iva_neto_cdo_a4_nd,nd.prop_neto_cdo_a4 as prop_neto_cdo_a4_nd,\
                      nd.iva_neto_cdo_a5 as iva_neto_cdo_a5_nd,nd.prop_neto_cdo_a5 as prop_neto_cdo_a5_nd,\
                      nd.total_cont as total_cont_nd,nd.prop_total_cont as prop_total_cont_nd,\
                      ce.iva_neto_cdo_a1 as iva_neto_cdo_a1_ce,ce.prop_neto_cdo_a1 as prop_neto_cdo_a1_ce,\
                      ce.iva_neto_cdo_a2 as iva_neto_cdo_a2_ce,ce.prop_neto_cdo_a2 as prop_neto_cdo_a2_ce,\
                      ce.iva_neto_cdo_a3 as iva_neto_cdo_a3_ce,ce.prop_neto_cdo_a3 as prop_neto_cdo_a3_ce,\
                      ce.iva_neto_cdo_a4 as iva_neto_cdo_a4_ce,ce.prop_neto_cdo_a4 as prop_neto_cdo_a4_ce,\
                      ce.iva_neto_cdo_a5 as iva_neto_cdo_a5_ce,ce.prop_neto_cdo_a5 as prop_neto_cdo_a5_ce,\
                      ce.total_cont as total_cont_ce,ce.prop_total_cont as prop_total_cont_ce,\
                      w08.iva_neto_cdo_a1 as iva_neto_cdo_a1_w08,w08.prop_neto_cdo_a1 as prop_neto_cdo_a1_w08,\
                      w08.iva_neto_cdo_a2 as iva_neto_cdo_a2_w08,w08.prop_neto_cdo_a2 as prop_neto_cdo_a2_w08,\
                      w08.iva_neto_cdo_a3 as iva_neto_cdo_a3_w08,w08.prop_neto_cdo_a3 as prop_neto_cdo_a3_w08,\
                      w08.iva_neto_cdo_a4 as iva_neto_cdo_a4_w08,w08.prop_neto_cdo_a4 as prop_neto_cdo_a4_w08,\
                      w08.iva_neto_cdo_a5 as iva_neto_cdo_a5_w08,w08.prop_neto_cdo_a5 as prop_neto_cdo_a5_w08,\
                      w08.total_cont as total_cont_w08,w08.prop_total_cont as prop_total_cont_w08,\
                      rem.iva_neto_cdo_a1 as iva_neto_cdo_a1_rem,rem.prop_neto_cdo_a1 as prop_neto_cdo_a1_rem,\
                      rem.iva_neto_cdo_a2 as iva_neto_cdo_a2_rem,rem.prop_neto_cdo_a2 as prop_neto_cdo_a2_rem,\
                      rem.iva_neto_cdo_a3 as iva_neto_cdo_a3_rem,rem.prop_neto_cdo_a3 as prop_neto_cdo_a3_rem,\
                      rem.iva_neto_cdo_a4 as iva_neto_cdo_a4_rem,rem.prop_neto_cdo_a4 as prop_neto_cdo_a4_rem,\
                      rem.iva_neto_cdo_a5 as iva_neto_cdo_a5_rem,rem.prop_neto_cdo_a5 as prop_neto_cdo_a5_rem,\
                      rem.total_cont as total_cont_rem,rem.prop_total_cont as prop_total_cont_rem,\
                      fm01.iva_neto_cdo_a1 as iva_neto_cdo_a1_fm01,fm01.prop_neto_cdo_a1 as prop_neto_cdo_a1_fm01,\
                      fm01.iva_neto_cdo_a2 as iva_neto_cdo_a2_fm01,fm01.prop_neto_cdo_a2 as prop_neto_cdo_a2_fm01,\
                      fm01.iva_neto_cdo_a3 as iva_neto_cdo_a3_fm01,fm01.prop_neto_cdo_a3 as prop_neto_cdo_a3_fm01,\
                      fm01.iva_neto_cdo_a4 as iva_neto_cdo_a4_fm01,fm01.prop_neto_cdo_a4 as prop_neto_cdo_a4_fm01,\
                      fm01.iva_neto_cdo_a5 as iva_neto_cdo_a5_fm01,fm01.prop_neto_cdo_a5 as prop_neto_cdo_a5_fm01,\
                      fm01.total_cont as total_cont_fm01,fm01.prop_total_cont as prop_total_cont_fm01, tr.c_537,\
                      ed30.iva_neto_cdo_a1 as iva_neto_cdo_a1_ed30,ed30.prop_neto_cdo_a1 as prop_neto_cdo_a1_ed30,\
                      ed30.iva_neto_cdo_a2 as iva_neto_cdo_a2_ed30,ed30.prop_neto_cdo_a2 as prop_neto_cdo_a2_ed30,\
                      ed30.iva_neto_cdo_a3 as iva_neto_cdo_a3_ed30,ed30.prop_neto_cdo_a3 as prop_neto_cdo_a3_ed30,\
                      ed30.iva_neto_cdo_a4 as iva_neto_cdo_a4_ed30,ed30.prop_neto_cdo_a4 as prop_neto_cdo_a4_ed30,\
                      ed30.iva_neto_cdo_a5 as iva_neto_cdo_a5_ed30,ed30.prop_neto_cdo_a5 as prop_neto_cdo_a5_ed30,\
                      ed30.total_cont as total_cont_ed30,ed30.prop_total_cont as prop_total_cont_ed30\
               from tmp_receptores as tr\
               left join tmp_resultado_resumen as tal on tr.receptor = tal.receptor \
                                                    and tr.dcv_ptributario = tal.dcv_ptributario and (coalesce(tal.origen_a1,0) = 2 or\
                                                                                    coalesce(tal.origen_a2,0) = 2 or\
                                                                                    coalesce(tal.origen_a3,0) = 2 or \
                                                                                    coalesce(tal.origen_a4,0) = 2 or\
                                                                                    coalesce(tal.origen_a5,0) = 2 )\
               left join tmp_resultado_resumen as ce on tr.receptor = ce.receptor \
                                                    and tr.dcv_ptributario = ce.dcv_ptributario and (coalesce(ce.origen_a1,0) =3 or\
                                                                                    coalesce(ce.origen_a2,0) =3 or\
                                                                                    coalesce(ce.origen_a3,0) =3 or\
                                                                                    coalesce(ce.origen_a4,0) =3 or\
                                                                                    coalesce(ce.origen_a5,0) = 3 )\
               left join tmp_resultado_resumen as nd on tr.receptor = nd.receptor \
                                                     and tr.dcv_ptributario = nd.dcv_ptributario and (coalesce(nd.origen_a1,0) = 1 or\
                                                                                   coalesce(nd.origen_a2,0) = 1 or\
                                                                                   coalesce(nd.origen_a3,0) = 1 or\
                                                                                   coalesce(nd.origen_a4,0) = 1 or\
                                                                                   coalesce(nd.origen_a5,0) = 1)\
               left join tmp_resultado_resumen as w08 on tr.receptor = w08.receptor \
                                                     and tr.dcv_ptributario = w08.dcv_ptributario and (coalesce(w08.origen_a1,0) = 4 or\
                                                                                   coalesce(w08.origen_a2,0) = 4 or\
                                                                                   coalesce(w08.origen_a3,0) = 4 or\
                                                                                   coalesce(w08.origen_a4,0) = 4 or\
                                                                                   coalesce(w08.origen_a5,0) = 4)\
               left join tmp_resultado_resumen as rem on tr.receptor = rem.receptor \
                                                     and tr.dcv_ptributario = rem.dcv_ptributario and (coalesce(rem.origen_a1,0) = 5 or\
                                                                                   coalesce(rem.origen_a2,0) = 5 or\
                                                                                   coalesce(rem.origen_a3,0) = 5 or\
                                                                                   coalesce(rem.origen_a4,0) = 5 or\
                                                                                   coalesce(rem.origen_a5,0) = 5)\
            left join tmp_resultado_resumen as ed30 on tr.receptor = ed30.receptor \
                                                     and tr.dcv_ptributario = ed30.dcv_ptributario and (coalesce(ed30.origen_a1,0) = 7 or\
                                                                                   coalesce(ed30.origen_a2,0) = 7 or\
                                                                                   coalesce(ed30.origen_a3,0) = 7 or\
                                                                                   coalesce(ed30.origen_a4,0) = 7 or\
                                                                                   coalesce(ed30.origen_a5,0) = 7)\
               left join tmp_resultado_resumen as fm01 on tr.receptor = fm01.receptor \
                                                     and tr.dcv_ptributario = fm01.dcv_ptributario and (coalesce(fm01.origen_a1,0) = 6 or\
                                                                                   coalesce(fm01.origen_a2,0) = 6 or\
                                                                                   coalesce(fm01.origen_a3,0) = 6 or\
                                                                                   coalesce(fm01.origen_a4,0) = 6 or\
                                                                                   coalesce(fm01.origen_a5,0) = 6)")

    #df.registerTempTable('tmp_receptores_resultado')
    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/iva_receptores_result")


    # In[73]:


    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/iva_receptores_result").registerTempTable('tmp_receptores_resultado')

    print("tmp_receptores_resultado ok")


    # In[74]:


    #spark.sql("select * from tmp_receptores_resultado").show()

                      # iva_neto_cdo_a1_al,prop_neto_cdo_a1_al,\
                      # iva_neto_cdo_a2_al,prop_neto_cdo_a2_al,iva_neto_cdo_a3_al,prop_neto_cdo_a3_al,\
                      # iva_neto_cdo_a4_al,prop_neto_cdo_a4_al,iva_neto_cdo_a5_al,prop_neto_cdo_a5_al,\
                      # total_cont_al,prop_total_cont_al,iva_neto_cdo_a1_nd,prop_neto_cdo_a1_nd,iva_neto_cdo_a2_nd,\
                      # prop_neto_cdo_a2_nd,iva_neto_cdo_a3_nd,prop_neto_cdo_a3_nd,iva_neto_cdo_a4_nd,\
                      # prop_neto_cdo_a4_nd,iva_neto_cdo_a5_nd,prop_neto_cdo_a5_nd,total_cont_nd,\
                      # prop_total_cont_nd,iva_neto_cdo_a1_ce,prop_neto_cdo_a1_ce,iva_neto_cdo_a2_ce,\
                      # prop_neto_cdo_a2_ce,iva_neto_cdo_a3_ce,prop_neto_cdo_a3_ce,iva_neto_cdo_a4_ce,\
                      # prop_neto_cdo_a4_ce,iva_neto_cdo_a5_ce,prop_neto_cdo_a5_ce,total_cont_ce,prop_total_cont_ce,\
                      # iva_neto_cdo_a1_w08,prop_neto_cdo_a1_w08,iva_neto_cdo_a2_w08,prop_neto_cdo_a2_w08,\
                      # iva_neto_cdo_a3_w08,prop_neto_cdo_a3_w08,iva_neto_cdo_a4_w08,prop_neto_cdo_a4_w08,\
                      # iva_neto_cdo_a5_w08,prop_neto_cdo_a5_w08,total_cont_w08,prop_total_cont_w08,\
                      # iva_neto_cdo_a1_rem, prop_neto_cdo_a1_rem, iva_neto_cdo_a2_rem, prop_neto_cdo_a2_rem,\
                      # iva_neto_cdo_a3_rem, prop_neto_cdo_a3_rem, iva_neto_cdo_a4_rem, prop_neto_cdo_a4_rem,\
                      # iva_neto_cdo_a5_rem, prop_neto_cdo_a5_rem, total_cont_rem, prop_total_cont_rem,\
                      # iva_neto_cdo_a1_fm01, prop_neto_cdo_a1_fm01, iva_neto_cdo_a2_fm01,prop_neto_cdo_a2_fm01,\
                      # iva_neto_cdo_a3_fm01, prop_neto_cdo_a3_fm01, iva_neto_cdo_a4_fm01,prop_neto_cdo_a4_fm01,\
                      # iva_neto_cdo_a5_fm01, prop_neto_cdo_a5_fm01, total_cont_fm01, prop_total_cont_fm01,\
                      # iva_neto_cdo_a1_an,\
                      # prop_neto_cdo_a1_an,iva_neto_cdo_a2_an,prop_neto_cdo_a2_an,iva_neto_cdo_a3_an,prop_neto_cdo_a3_an,\
                      # iva_neto_cdo_a4_an,prop_neto_cdo_a4_an,iva_neto_cdo_a5_an,prop_neto_cdo_a5_an,total_cont_an,\
                      # prop_total_cont_an,c_537,\
                      # created,user\

    df=spark.sql("select *,\
                        coalesce(iva_neto_cdo_a1_al,0)+coalesce(iva_neto_cdo_a1_rem,0)+coalesce(iva_neto_cdo_a1_nd,0)+coalesce(iva_neto_cdo_a1_w08,0)+coalesce(iva_neto_cdo_a1_fm01,0)+coalesce(iva_neto_cdo_a1_ed30,0) as iva_neto_cdo_a1_an,\
                        coalesce(iva_neto_cdo_a2_al,0)+coalesce(iva_neto_cdo_a2_rem,0)+coalesce(iva_neto_cdo_a2_nd,0)+coalesce(iva_neto_cdo_a2_w08,0)+coalesce(iva_neto_cdo_a2_fm01,0)+coalesce(iva_neto_cdo_a2_ed30,0) as iva_neto_cdo_a2_an,\
                        coalesce(iva_neto_cdo_a3_al,0)+coalesce(iva_neto_cdo_a3_rem,0)+coalesce(iva_neto_cdo_a3_nd,0)+coalesce(iva_neto_cdo_a3_w08,0)+coalesce(iva_neto_cdo_a3_fm01,0)+coalesce(iva_neto_cdo_a3_ed30,0) as iva_neto_cdo_a3_an,\
                        coalesce(iva_neto_cdo_a4_al,0)+coalesce(iva_neto_cdo_a4_rem,0)+coalesce(iva_neto_cdo_a4_nd,0)+coalesce(iva_neto_cdo_a4_w08,0)+coalesce(iva_neto_cdo_a4_fm01,0)+coalesce(iva_neto_cdo_a4_ed30,0) as iva_neto_cdo_a4_an,\
                        coalesce(iva_neto_cdo_a5_al,0)+coalesce(iva_neto_cdo_a5_rem,0)+coalesce(iva_neto_cdo_a5_nd,0)+coalesce(iva_neto_cdo_a5_w08,0)+coalesce(iva_neto_cdo_a5_fm01,0)+coalesce(iva_neto_cdo_a5_ed30,0) as iva_neto_cdo_a5_an,\
                        coalesce(prop_neto_cdo_a1_al,0)+coalesce(prop_neto_cdo_a1_rem,0)+coalesce(prop_neto_cdo_a1_nd,0)+coalesce(prop_neto_cdo_a1_w08,0)+coalesce(prop_neto_cdo_a1_fm01,0)+coalesce(prop_neto_cdo_a1_ed30,0) as prop_neto_cdo_a1_an,\
                        coalesce(prop_neto_cdo_a2_al,0)+coalesce(prop_neto_cdo_a2_rem,0)+coalesce(prop_neto_cdo_a2_nd,0)+coalesce(prop_neto_cdo_a2_w08,0)+coalesce(prop_neto_cdo_a2_fm01,0)+coalesce(prop_neto_cdo_a2_ed30,0) as prop_neto_cdo_a2_an,\
                        coalesce(prop_neto_cdo_a3_al,0)+coalesce(prop_neto_cdo_a3_rem,0)+coalesce(prop_neto_cdo_a3_nd,0)+coalesce(prop_neto_cdo_a3_w08,0)+coalesce(prop_neto_cdo_a3_fm01,0)+coalesce(prop_neto_cdo_a3_ed30,0) as prop_neto_cdo_a3_an,\
                        coalesce(prop_neto_cdo_a4_al,0)+coalesce(prop_neto_cdo_a4_rem,0)+coalesce(prop_neto_cdo_a4_nd,0)+coalesce(prop_neto_cdo_a4_w08,0)+coalesce(prop_neto_cdo_a4_fm01,0)+coalesce(prop_neto_cdo_a4_ed30,0) as prop_neto_cdo_a4_an,\
                        coalesce(prop_neto_cdo_a5_al,0)+coalesce(prop_neto_cdo_a5_rem,0)+coalesce(prop_neto_cdo_a5_nd,0)+coalesce(prop_neto_cdo_a5_w08,0)+coalesce(prop_neto_cdo_a5_fm01,0)+coalesce(prop_neto_cdo_a5_ed30,0) as prop_neto_cdo_a5_an\
                 from tmp_receptores_resultado")

    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/iva_receptores_result2")


    # In[75]:


    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/iva_receptores_result2").registerTempTable('tmp_receptores_resultado2')

    print("tmp_receptores_resultado2 ok")


    # In[76]:


    # tmp_receptores_resultado2


    # In[77]:


    ##@@@@
    df=spark.sql("select *, \
                        coalesce(iva_neto_cdo_a1_an,0) + coalesce(iva_neto_cdo_a2_an,0)+coalesce(iva_neto_cdo_a3_an,0)+\
                        coalesce(iva_neto_cdo_a4_an,0)+coalesce(iva_neto_cdo_a5_an,0) as total_cont_an,\
                        prop_neto_cdo_a1_an+prop_neto_cdo_a2_an+prop_neto_cdo_a3_an+prop_neto_cdo_a4_an+prop_neto_cdo_a5_an as prop_total_cont_an\
                 from tmp_receptores_resultado2")

    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/resultado_etapa3")


    # In[78]:


    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/resultado_etapa3").registerTempTable('tmp_resultado_etapa3')

    print("tmp_resultado_etapa3 ok")

    ## Saltar hasta @@@


    # In[79]:


    #@@@
    if exists_hist==1:
        # ACtualización de datos históricos
        # 2. Se crea temporal excluyendo los periodos en procesamiento (de parámtros)
        df=spark.sql(f"select * \
                  from tmp_historico \
                  where dcv_ptributario not in ({Periodo})")

        df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/hist_parcial")


    # In[80]:


    if exists_hist==1:
        spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/hist_parcial").registerTempTable('tmp_hist_parcial')
        print("lectura de tmp_historico parcial ok")


    # In[81]:


    if exists_hist==1:
        df=spark.sql("select dcv_ptributario,receptor,f29_neto_reci,\
                      iva_neto_cdo_a1_al,prop_neto_cdo_a1_al,\
                      iva_neto_cdo_a2_al,prop_neto_cdo_a2_al,iva_neto_cdo_a3_al,prop_neto_cdo_a3_al,\
                      iva_neto_cdo_a4_al,prop_neto_cdo_a4_al,iva_neto_cdo_a5_al,prop_neto_cdo_a5_al,\
                      total_cont_al,prop_total_cont_al,iva_neto_cdo_a1_nd,prop_neto_cdo_a1_nd,iva_neto_cdo_a2_nd,\
                      prop_neto_cdo_a2_nd,iva_neto_cdo_a3_nd,prop_neto_cdo_a3_nd,iva_neto_cdo_a4_nd,\
                      prop_neto_cdo_a4_nd,iva_neto_cdo_a5_nd,prop_neto_cdo_a5_nd,total_cont_nd,\
                      prop_total_cont_nd,iva_neto_cdo_a1_ce,prop_neto_cdo_a1_ce,iva_neto_cdo_a2_ce,\
                      prop_neto_cdo_a2_ce,iva_neto_cdo_a3_ce,prop_neto_cdo_a3_ce,iva_neto_cdo_a4_ce,\
                      prop_neto_cdo_a4_ce,iva_neto_cdo_a5_ce,prop_neto_cdo_a5_ce,total_cont_ce,prop_total_cont_ce,\
                      iva_neto_cdo_a1_w08,prop_neto_cdo_a1_w08,iva_neto_cdo_a2_w08,prop_neto_cdo_a2_w08,\
                      iva_neto_cdo_a3_w08,prop_neto_cdo_a3_w08,iva_neto_cdo_a4_w08,prop_neto_cdo_a4_w08,\
                      iva_neto_cdo_a5_w08,prop_neto_cdo_a5_w08,total_cont_w08,prop_total_cont_w08,\
                      iva_neto_cdo_a1_ed30,prop_neto_cdo_a1_ed30,iva_neto_cdo_a2_ed30,prop_neto_cdo_a2_ed30,\
                      iva_neto_cdo_a3_ed30,prop_neto_cdo_a3_ed30,iva_neto_cdo_a4_ed30,prop_neto_cdo_a4_ed30,\
                      iva_neto_cdo_a5_ed30,prop_neto_cdo_a5_ed30,total_cont_ed30,prop_total_cont_ed30,\
                      iva_neto_cdo_a1_rem, prop_neto_cdo_a1_rem, iva_neto_cdo_a2_rem, prop_neto_cdo_a2_rem,\
                      iva_neto_cdo_a3_rem, prop_neto_cdo_a3_rem, iva_neto_cdo_a4_rem, prop_neto_cdo_a4_rem,\
                      iva_neto_cdo_a5_rem, prop_neto_cdo_a5_rem, total_cont_rem, prop_total_cont_rem,\
                      iva_neto_cdo_a1_fm01, prop_neto_cdo_a1_fm01, iva_neto_cdo_a2_fm01,prop_neto_cdo_a2_fm01,\
                      iva_neto_cdo_a3_fm01, prop_neto_cdo_a3_fm01, iva_neto_cdo_a4_fm01,prop_neto_cdo_a4_fm01,\
                      iva_neto_cdo_a5_fm01, prop_neto_cdo_a5_fm01, total_cont_fm01, prop_total_cont_fm01,\
                      iva_neto_cdo_a1_an,\
                      prop_neto_cdo_a1_an,iva_neto_cdo_a2_an,prop_neto_cdo_a2_an,iva_neto_cdo_a3_an,prop_neto_cdo_a3_an,\
                      iva_neto_cdo_a4_an,prop_neto_cdo_a4_an,iva_neto_cdo_a5_an,prop_neto_cdo_a5_an,total_cont_an,\
                      prop_total_cont_an,c_537,\
                      created,user\
                  from tmp_hist_parcial\
               union\
               select dcv_ptributario,receptor,f29_neto_reci,iva_neto_cdo_a1_al,prop_neto_cdo_a1_al,\
                      iva_neto_cdo_a2_al,prop_neto_cdo_a2_al,iva_neto_cdo_a3_al,prop_neto_cdo_a3_al,\
                      iva_neto_cdo_a4_al,prop_neto_cdo_a4_al,iva_neto_cdo_a5_al,prop_neto_cdo_a5_al,\
                      total_cont_al,prop_total_cont_al,iva_neto_cdo_a1_nd,prop_neto_cdo_a1_nd,iva_neto_cdo_a2_nd,\
                      prop_neto_cdo_a2_nd,iva_neto_cdo_a3_nd,prop_neto_cdo_a3_nd,iva_neto_cdo_a4_nd,\
                      prop_neto_cdo_a4_nd,iva_neto_cdo_a5_nd,prop_neto_cdo_a5_nd,total_cont_nd,\
                      prop_total_cont_nd,iva_neto_cdo_a1_ce,prop_neto_cdo_a1_ce,iva_neto_cdo_a2_ce,\
                      prop_neto_cdo_a2_ce,iva_neto_cdo_a3_ce,prop_neto_cdo_a3_ce,iva_neto_cdo_a4_ce,\
                      prop_neto_cdo_a4_ce,iva_neto_cdo_a5_ce,prop_neto_cdo_a5_ce,total_cont_ce,prop_total_cont_ce,\
                      iva_neto_cdo_a1_w08,prop_neto_cdo_a1_w08,iva_neto_cdo_a2_w08,prop_neto_cdo_a2_w08,\
                      iva_neto_cdo_a3_w08,prop_neto_cdo_a3_w08,iva_neto_cdo_a4_w08,prop_neto_cdo_a4_w08,\
                      iva_neto_cdo_a5_w08,prop_neto_cdo_a5_w08,total_cont_w08,prop_total_cont_w08,\
                      iva_neto_cdo_a1_ed30,prop_neto_cdo_a1_ed30,iva_neto_cdo_a2_ed30,prop_neto_cdo_a2_ed30,\
                      iva_neto_cdo_a3_ed30,prop_neto_cdo_a3_ed30,iva_neto_cdo_a4_ed30,prop_neto_cdo_a4_ed30,\
                      iva_neto_cdo_a5_ed30,prop_neto_cdo_a5_ed30,total_cont_ed30,prop_total_cont_ed30,\
                      iva_neto_cdo_a1_rem,prop_neto_cdo_a1_rem,iva_neto_cdo_a2_rem,prop_neto_cdo_a2_rem,\
                      iva_neto_cdo_a3_rem,prop_neto_cdo_a3_rem,iva_neto_cdo_a4_rem,prop_neto_cdo_a4_rem,\
                      iva_neto_cdo_a5_rem,prop_neto_cdo_a5_rem,total_cont_rem,prop_total_cont_rem,\
                      iva_neto_cdo_a1_fm01, prop_neto_cdo_a1_fm01, iva_neto_cdo_a2_fm01, prop_neto_cdo_a2_fm01,\
                      iva_neto_cdo_a3_fm01, prop_neto_cdo_a3_fm01, iva_neto_cdo_a4_fm01, prop_neto_cdo_a4_fm01,\
                      iva_neto_cdo_a5_fm01, prop_neto_cdo_a5_fm01, total_cont_fm01, prop_total_cont_fm01,\
                      iva_neto_cdo_a1_an,\
                      prop_neto_cdo_a1_an,iva_neto_cdo_a2_an,prop_neto_cdo_a2_an,iva_neto_cdo_a3_an,prop_neto_cdo_a3_an,\
                      iva_neto_cdo_a4_an,prop_neto_cdo_a4_an,iva_neto_cdo_a5_an,prop_neto_cdo_a5_an,total_cont_an,\
                      prop_total_cont_an,c_537,\
                      current_timestamp,current_user()\
                  from tmp_resultado_etapa3")

    if exists_hist==0:
        df=spark.sql("select dcv_ptributario,receptor,f29_neto_reci,iva_neto_cdo_a1_al,prop_neto_cdo_a1_al,\
                      iva_neto_cdo_a2_al,prop_neto_cdo_a2_al,iva_neto_cdo_a3_al,prop_neto_cdo_a3_al,\
                      iva_neto_cdo_a4_al,prop_neto_cdo_a4_al,iva_neto_cdo_a5_al,prop_neto_cdo_a5_al,\
                      total_cont_al,prop_total_cont_al,iva_neto_cdo_a1_nd,prop_neto_cdo_a1_nd,iva_neto_cdo_a2_nd,\
                      prop_neto_cdo_a2_nd,iva_neto_cdo_a3_nd,prop_neto_cdo_a3_nd,iva_neto_cdo_a4_nd,\
                      prop_neto_cdo_a4_nd,iva_neto_cdo_a5_nd,prop_neto_cdo_a5_nd,total_cont_nd,\
                      prop_total_cont_nd,iva_neto_cdo_a1_ce,prop_neto_cdo_a1_ce,iva_neto_cdo_a2_ce,\
                      prop_neto_cdo_a2_ce,iva_neto_cdo_a3_ce,prop_neto_cdo_a3_ce,iva_neto_cdo_a4_ce,\
                      prop_neto_cdo_a4_ce,iva_neto_cdo_a5_ce,prop_neto_cdo_a5_ce,total_cont_ce,prop_total_cont_ce,\
                      iva_neto_cdo_a1_w08,prop_neto_cdo_a1_w08,iva_neto_cdo_a2_w08,prop_neto_cdo_a2_w08,\
                      iva_neto_cdo_a3_w08,prop_neto_cdo_a3_w08,iva_neto_cdo_a4_w08,prop_neto_cdo_a4_w08,\
                      iva_neto_cdo_a5_w08,prop_neto_cdo_a5_w08,total_cont_w08,prop_total_cont_w08,\
                      iva_neto_cdo_a1_ed30,prop_neto_cdo_a1_ed30,iva_neto_cdo_a2_ed30,prop_neto_cdo_a2_ed30,\
                      iva_neto_cdo_a3_ed30,prop_neto_cdo_a3_ed30,iva_neto_cdo_a4_ed30,prop_neto_cdo_a4_ed30,\
                      iva_neto_cdo_a5_ed30,prop_neto_cdo_a5_ed30,total_cont_ed30,prop_total_cont_ed30,\
                      iva_neto_cdo_a1_rem,prop_neto_cdo_a1_rem,iva_neto_cdo_a2_rem,prop_neto_cdo_a2_rem,\
                      iva_neto_cdo_a3_rem,prop_neto_cdo_a3_rem,iva_neto_cdo_a4_rem,prop_neto_cdo_a4_rem,\
                      iva_neto_cdo_a5_rem,prop_neto_cdo_a5_rem,total_cont_rem,prop_total_cont_rem,\
                      iva_neto_cdo_a1_fm01, prop_neto_cdo_a1_fm01, iva_neto_cdo_a2_fm01, prop_neto_cdo_a2_fm01,\
                      iva_neto_cdo_a3_fm01, prop_neto_cdo_a3_fm01, iva_neto_cdo_a4_fm01, prop_neto_cdo_a4_fm01,\
                      iva_neto_cdo_a5_fm01, prop_neto_cdo_a5_fm01, total_cont_fm01, prop_total_cont_fm01,\
                      iva_neto_cdo_a1_an,\
                      prop_neto_cdo_a1_an,iva_neto_cdo_a2_an,prop_neto_cdo_a2_an,iva_neto_cdo_a3_an,prop_neto_cdo_a3_an,\
                      iva_neto_cdo_a4_an,prop_neto_cdo_a4_an,iva_neto_cdo_a5_an,prop_neto_cdo_a5_an,total_cont_an,\
                      prop_total_cont_an,c_537,\
                      current_timestamp as created,current_user() as user\
                  from tmp_resultado_etapa3")

    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/historico_updated")
    print("nueva version de tmp_historico ok")


    # In[82]:


    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/historico_updated").registerTempTable('tmp_historico')


    # In[83]:


    if exists_hist!=0:
        for i, arg in enumerate(sys.argv[1:], start=1):
                Periodo = arg
                print("Extrayendo Resultado Periodo: ", Periodo)
                #df=spark.sql(f"select * \
                #               from tmp_resultado_etapa3 \
                #               where dcv_ptributario = '{Periodo}'")

                df=spark.sql(f"select t1.dcv_ptributario, t1.receptor, f29_neto_reci,\
                                      t2.periodo_ant, \
                                      t2.f29_77_ant, t2.prop_total_cont_an_ant,t2.prop_remanente_cont as remanente_cont,\
                                      t3.F29_c_504,\
                                      iva_neto_cdo_a1_rem,prop_neto_cdo_a1_rem,\
                                      iva_neto_cdo_a2_rem,prop_neto_cdo_a2_rem,\
                                      iva_neto_cdo_a3_rem,prop_neto_cdo_a3_rem,\
                                      iva_neto_cdo_a4_rem,prop_neto_cdo_a4_rem,\
                                      iva_neto_cdo_a5_rem,prop_neto_cdo_a5_rem,\
                                      total_cont_rem,prop_total_cont_rem \
                               from tmp_resultado_etapa3 as t1\
                               left join tmp_remanente_ant as t2 on t1.receptor = t2.receptor\
                               left join tmp_f29 as t3 on t1.receptor = t3.cont_rut\
                               where dcv_ptributario = '{Periodo}'")        
                pd=df.toPandas()
                pd.to_csv('data/iva_remanente/IVA_Credito'+Periodo+'.csv',index=False)

        print("Extracción de Archivo ok")


    # In[84]:


    ####################################################################################
    ################### INICIO Tratamiento de Remanente histórico ######################
    ####################################################################################


    # In[85]:


    print("*****Inicio de Proceso de Mantención de Remanente Histórico*****")


    # In[86]:


    #tmp_historico: Carga de REsultados histórico
    try:
        spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/remanente_hist_updated")\
         .registerTempTable('tmp_remanente_hist')
        print("Archivo cargado y tabla temporal registrada exitosamente.")
        exists_rema_hist=1
    except AnalysisException as e:
        if 'Path does not exist' in str(e):
            print("El archivo no está disponible en la ruta especificada.")
            exists_rema_hist=0
        else:
            print("Ocurrió un error al intentar leer el archivo:", str(e))

        # Lectura de Remanente Histórico


    # In[87]:


    # ##############################################################################################################
    # ############ Porción de Código para recrear Remanente Histórico en caso de pérdida del Archivo ###############
    # ##############################################################################################################


    # Priodo = 202108
    df=spark.sql(f"select t1.dcv_ptributario, t1.receptor, f29_neto_reci,\
                      t2.periodo_ant, \
                      t2.f29_77_ant, t2.prop_total_cont_an_ant,t2.prop_remanente_cont as remanente_cont,\
                      t2.F29_c_504,\
                      iva_neto_cdo_a1_rem,prop_neto_cdo_a1_rem,\
                      iva_neto_cdo_a2_rem,prop_neto_cdo_a2_rem,\
                      iva_neto_cdo_a3_rem,prop_neto_cdo_a3_rem,\
                      iva_neto_cdo_a4_rem,prop_neto_cdo_a4_rem,\
                      iva_neto_cdo_a5_rem,prop_neto_cdo_a5_rem,\
                      total_cont_rem,prop_total_cont_rem,\
                      iva_neto_cdo_a1_nd,prop_neto_cdo_a1_nd,\
                      iva_neto_cdo_a2_nd,prop_neto_cdo_a2_nd,\
                      iva_neto_cdo_a3_nd,prop_neto_cdo_a3_nd,\
                      iva_neto_cdo_a4_nd,prop_neto_cdo_a4_nd,\
                      iva_neto_cdo_a5_nd,prop_neto_cdo_a5_nd,\
                      total_cont_nd,prop_total_cont_nd,\
                      iva_neto_cdo_a1_al,prop_neto_cdo_a1_al,\
                      iva_neto_cdo_a2_al,prop_neto_cdo_a2_al,\
                      iva_neto_cdo_a3_al,prop_neto_cdo_a3_al,\
                      iva_neto_cdo_a4_al,prop_neto_cdo_a4_al,\
                      iva_neto_cdo_a5_al,prop_neto_cdo_a5_al,\
                      total_cont_al,prop_total_cont_al,\
                      iva_neto_cdo_a1_w08,prop_neto_cdo_a1_w08,\
                      iva_neto_cdo_a2_w08,prop_neto_cdo_a2_w08,\
                      iva_neto_cdo_a3_w08,prop_neto_cdo_a3_w08,\
                      iva_neto_cdo_a4_w08,prop_neto_cdo_a4_w08,\
                      iva_neto_cdo_a5_w08,prop_neto_cdo_a5_w08,\
                      total_cont_w08,prop_total_cont_w08,\
                      iva_neto_cdo_a1_ed30,prop_neto_cdo_a1_ed30,\
                      iva_neto_cdo_a2_ed30,prop_neto_cdo_a2_ed30,\
                      iva_neto_cdo_a3_ed30,prop_neto_cdo_a3_ed30,\
                      iva_neto_cdo_a4_ed30,prop_neto_cdo_a4_ed30,\
                      iva_neto_cdo_a5_ed30,prop_neto_cdo_a5_ed30,\
                      total_cont_ed30,prop_total_cont_ed30,\
                      iva_neto_cdo_a1_ce,prop_neto_cdo_a1_ce,\
                      iva_neto_cdo_a2_ce,prop_neto_cdo_a2_ce,\
                      iva_neto_cdo_a3_ce,prop_neto_cdo_a3_ce,\
                      iva_neto_cdo_a4_ce,prop_neto_cdo_a4_ce,\
                      iva_neto_cdo_a5_ce,prop_neto_cdo_a5_ce,\
                      total_cont_ce,prop_total_cont_ce,\
                       iva_neto_cdo_a1_fm01,prop_neto_cdo_a1_fm01,\
                       iva_neto_cdo_a2_fm01,prop_neto_cdo_a2_fm01,\
                       iva_neto_cdo_a3_fm01,prop_neto_cdo_a3_fm01,\
                       iva_neto_cdo_a4_fm01,prop_neto_cdo_a4_fm01,\
                       iva_neto_cdo_a5_fm01,prop_neto_cdo_a5_fm01,\
                       total_cont_fm01,prop_total_cont_fm01,\
                      iva_neto_cdo_a1_an,prop_neto_cdo_a1_an,\
                      iva_neto_cdo_a2_an,prop_neto_cdo_a2_an,\
                      iva_neto_cdo_a3_an,prop_neto_cdo_a3_an,\
                      iva_neto_cdo_a4_an,prop_neto_cdo_a4_an,\
                      iva_neto_cdo_a5_an,prop_neto_cdo_a5_an,\
                      total_cont_an,prop_total_cont_an\
                from tmp_historico as t1\
                left join tmp_remanente_ant as t2 on t1.receptor = t2.receptor\
                where dcv_ptributario = '{Periodo}'\
                order by t1.dcv_ptributario asc")


    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/remanente_hist_updated")

    #Carga tabla tmp_remanente_hist_updated
    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/remanente_hist_updated")\
     .registerTempTable('tmp_remanente_hist')




    # In[88]:


    ### Respaldo de Remanente Histórico ###
    df=spark.sql("select * from tmp_remanente_hist")
    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/remanente_hist_resp")


    # In[89]:


    #tmp_historico: Carga de REsultados histórico
    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/historico_updated")\
              .registerTempTable('tmp_historico')


    # In[90]:


    # Aislamiento de Periodo Actual para posterior Actualización en Archivo de historia
    df=spark.sql(f"select *\
                  from tmp_remanente_hist\
                  where dcv_ptributario != '{Periodo}'")

    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/remanente_parcial")

    print("Remanente de Periodos Diferntes a " + str(Periodo) + " ok")


    # In[91]:


    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/remanente_parcial")\
         .registerTempTable('tmp_remanente_parcial')


    # In[92]:


    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/remanente_ant_updated")\
         .registerTempTable('tmp_remanente_ant')


    # In[93]:


    spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/f29_1")\
         .registerTempTable('tmp_f29_1')


    # In[94]:


    # Actualización de Remanente Histórico de información del mes en proceso
    df=spark.sql(f"select *\
                  from tmp_remanente_parcial as t1\
                  union\
                  select t1.dcv_ptributario, t1.receptor, f29_neto_reci,\
                          t2.periodo_ant, \
                          t2.f29_77_ant, t2.prop_total_cont_an_ant,t2.prop_remanente_cont as remanente_cont,\
                          t3.F29_c_504,\
                          iva_neto_cdo_a1_rem,prop_neto_cdo_a1_rem,\
                          iva_neto_cdo_a2_rem,prop_neto_cdo_a2_rem,\
                          iva_neto_cdo_a3_rem,prop_neto_cdo_a3_rem,\
                          iva_neto_cdo_a4_rem,prop_neto_cdo_a4_rem,\
                          iva_neto_cdo_a5_rem,prop_neto_cdo_a5_rem,\
                          total_cont_rem,prop_total_cont_rem,\
                           iva_neto_cdo_a1_nd,prop_neto_cdo_a1_nd,\
                           iva_neto_cdo_a2_nd,prop_neto_cdo_a2_nd,\
                           iva_neto_cdo_a3_nd,prop_neto_cdo_a3_nd,\
                           iva_neto_cdo_a4_nd,prop_neto_cdo_a4_nd,\
                           iva_neto_cdo_a5_nd,prop_neto_cdo_a5_nd,\
                           total_cont_nd,prop_total_cont_nd,\
                           iva_neto_cdo_a1_al,prop_neto_cdo_a1_al,\
                           iva_neto_cdo_a2_al,prop_neto_cdo_a2_al,\
                           iva_neto_cdo_a3_al,prop_neto_cdo_a3_al,\
                           iva_neto_cdo_a4_al,prop_neto_cdo_a4_al,\
                           iva_neto_cdo_a5_al,prop_neto_cdo_a5_al,\
                           total_cont_al,prop_total_cont_al,\
                           iva_neto_cdo_a1_w08,prop_neto_cdo_a1_w08,\
                           iva_neto_cdo_a2_w08,prop_neto_cdo_a2_w08,\
                           iva_neto_cdo_a3_w08,prop_neto_cdo_a3_w08,\
                           iva_neto_cdo_a4_w08,prop_neto_cdo_a4_w08,\
                           iva_neto_cdo_a5_w08,prop_neto_cdo_a5_w08,\
                           total_cont_w08,prop_total_cont_w08,\
                           iva_neto_cdo_a1_ed30,prop_neto_cdo_a1_ed30,\
                           iva_neto_cdo_a2_ed30,prop_neto_cdo_a2_ed30,\
                           iva_neto_cdo_a3_ed30,prop_neto_cdo_a3_ed30,\
                           iva_neto_cdo_a4_ed30,prop_neto_cdo_a4_ed30,\
                           iva_neto_cdo_a5_ed30,prop_neto_cdo_a5_ed30,\
                           total_cont_ed30,prop_total_cont_ed30,\
                           iva_neto_cdo_a1_ce,prop_neto_cdo_a1_ce,\
                           iva_neto_cdo_a2_ce,prop_neto_cdo_a2_ce,\
                           iva_neto_cdo_a3_ce,prop_neto_cdo_a3_ce,\
                           iva_neto_cdo_a4_ce,prop_neto_cdo_a4_ce,\
                           iva_neto_cdo_a5_ce,prop_neto_cdo_a5_ce,\
                           total_cont_ce,prop_total_cont_ce,\
                           iva_neto_cdo_a1_fm01,prop_neto_cdo_a1_fm01,\
                           iva_neto_cdo_a2_fm01,prop_neto_cdo_a2_fm01,\
                           iva_neto_cdo_a3_fm01,prop_neto_cdo_a3_fm01,\
                           iva_neto_cdo_a4_fm01,prop_neto_cdo_a4_fm01,\
                           iva_neto_cdo_a5_fm01,prop_neto_cdo_a5_fm01,\
                           total_cont_fm01,prop_total_cont_fm01,\
                           iva_neto_cdo_a1_an,prop_neto_cdo_a1_an,\
                           iva_neto_cdo_a2_an,prop_neto_cdo_a2_an,\
                           iva_neto_cdo_a3_an,prop_neto_cdo_a3_an,\
                           iva_neto_cdo_a4_an,prop_neto_cdo_a4_an,\
                           iva_neto_cdo_a5_an,prop_neto_cdo_a5_an,\
                           total_cont_an,prop_total_cont_an\
                    from tmp_historico as t1\
                    left join tmp_remanente_ant as t2 on t1.receptor = t2.receptor and t1.dcv_ptributario = t2.periodo\
                    left join tmp_f29_1 as t3 on t1.receptor = t3.cont_rut\
                    left join tmp_remanente_ant as t4 on t1.receptor = t4.receptor and t4.periodo_ant = '{Periodo_Ant}'\
                    where t1.dcv_ptributario = '{Periodo}'") 

    df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/iva_credito/intermedia/remanente_hist_updated")

    print("Actualización de Remanente ok")
    print(f"******** Fin del Proceso Periodo {Periodo}********")
spark.stop()
print("******** Fin del Proceso Completo******")
