# -*- coding: utf-8 -*-
from pyspark.sql.window import Window
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, functions
from pyspark.sql.functions import *
import pandas
import sys
import time
import os
import datetime
from pyspark.sql.types import *
from pyspark.sql import HiveContext, SQLContext, SparkSession
import os, logging, sys

# reload(sys)
# sys.setdefaultencoding('utf8')

# Configuraci�n de la sesi�n de Spark
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sqlContext = SQLContext.getOrCreate(spark)

# Configuraci�n del fichero de log
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s : %(levelname)s : %(message)s')

# Variables proceso
ruta_actual = os.getcwd()
pos_x = ruta_actual.find("/n")
usuario = ruta_actual[(pos_x + 1):(pos_x + 8)]

#######################################################################################################################



def Ind_bloqueo():

    df_f3_loaniq = sqlContext.sql(
        "select translate(concat(idemprcf,idcentcf,idprodcf,idcontrf), ' ', '') as partenon_iq from dlss_arqtec_confsw_desptc_h.objtemp_prima_loaniq where data_date_part = '" + fecha_datos + "' and substr(tipo_perf, 1, 1) != 'P'")
    df_f3_fyc = sqlContext.sql(
        "select concat(trim(idemprcf),trim(idcentcf),trim(idprodcf),trim(idcontrf)) as partenon_fyc, "
        "trim(idobjf) as garantia_fyc from dlss_ppss_fin_factoring_h.objtemp_prima_fyc where data_date_part = '" + fecha_datos + "'")

    df_f3_vigor = sqlContext.sql(
        "select tipooper as tipo_perf, translate(concat(idemprc,idcentc,idprodc,idcontr), ' ', '') as partenon, translate(concat(idemprp,idcentp,anoprop, "
        "lpad(numprop, 5, '0')), ' ', '') as propuesta, tipobj as tipobjf, idobj as idobjf,   tipgar as tipgarf, fecalta as fecaltaf,"
        "fecbaja as fecbajaf, valoraci as valoracif, indbloq as indbloqf, tipinstr as tipinstrf, porcrepar as porcreparf, valoract as valoractf, valoreal as valorealf, "
        " num_tit_val/10 as numtitf from dlss_risk_eerr_segcore_h.garantias_reales where data_date_part = '" + fecha_datos + "'"
                                                                                                                                                                                           " and tipooper != 'PR' and fecbaja >= '" + fecha_datos + "' and idcentc not in ('3291','3293','3294','3295','3296')")

    df_f3_vigor = df_f3_vigor.join(df_f3_loaniq, [df_f3_vigor.partenon == df_f3_loaniq.partenon_iq], 'left').select(df_f3_vigor["*"], df_f3_loaniq["partenon_iq"])
    df_f3_vigor = df_f3_vigor.withColumn('loaniq',functions.when(df_f3_vigor.partenon_iq.isNotNull(), lit("SI")).otherwise(lit("NO"))).drop('partenon_iq')
    df_f3_vigor = df_f3_vigor.join(df_f3_fyc, [df_f3_vigor.partenon == df_f3_fyc.partenon_fyc,df_f3_vigor.garantia == df_f3_fyc.garantia_fyc], 'left').select(df_f3_vigor['*'], df_f3_fyc["*"])
    df_f3_vigor = df_f3_vigor.withColumn('fyc_externo',functions.when(df_f3_vigor.partenon_fyc.isNotNull(), lit("SI")).otherwise(lit("NO"))).drop('garantia_fyc').drop('partenon_fyc')

    df_f3_vigor_fb = df_f3_vigor.filter(df_f3_vigor.tipobjf.isin('CONTRATO_OTRAENT', 'DESCRIPCION_BREVE_OTRAENT', 'SEGUROS_OTRAENT','SUBCONTRATO_OTRAENT'))
    df_f3_vigor_fb = df_f3_vigor_fb.withColumn('tipo',functions.when(df_f3_vigor_fb.tipo_perf[1:1] == 'P', lit("propuesta marco")).otherwise(lit("contrato")))
    df_f3_vigor_fb_loan = df_f3_vigor_fb.filter((df_f3_vigor_fb.loaniq == 'SI') | (df_f3_vigor_fb.fyc_externo == 'SI'))
    df_f3_vigor_fuera_banco = df_f3_vigor_fb.filter((df_f3_vigor_fb.loaniq == 'NO') & (df_f3_vigor_fb.fyc_externo == 'NO'))
    df_f3_vigor_fb_loan.repartition(5).write.mode("overwrite").saveAsTable("ws_mdc_seg.df_f3_vigor_fb_loan")
    print('df_f3_vigor_fb_loan')
    df_f3_vigor_fuera_banco.repartition(5).write.mode("overwrite").saveAsTable("ws_mdc_seg.df_f3_vigor_fuera_banco")
    print('df_f3_vigor_fuera_banco')


#######################################################################################################################
# Definición variables
#######################################################################################################################

fecha_datos = '2020-11-16'

#######################################################################################################################
# Ejecución de controles:
###############################################################################################################

ruta_actual = os.getcwd()
pos_x = ruta_actual.find("/n")
usuario = ruta_actual[(pos_x + 1):(pos_x + 8)]

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s : %(levelname)s : %(message)s')

logging.info('INICIO - Indicador de bloqueo')
Ind_bloqueo()
logging.info('FINAL Indicador de bloqueo')

# Vaciar la papelera
os.system("hdfs dfs -rm -r -skipTrash /user/" + usuario + "/.Trash/")
