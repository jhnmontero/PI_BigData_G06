# Databricks notebook source
# MAGIC %md
# MAGIC # ejecutar_todo · Flujo completo del Proyecto Integrador · Grupo 06
# MAGIC Corre de principio a fin: bronce, plata, anónima, oro, modelo, optimización, visualización, integración y monitoreo. Es el notebook del trabajo programado (*Job*).

# COMMAND ----------

import time

INICIO_EJECUCION = time.time()
tiempos_etapas = []
_marca = [time.time()]


def marca(etapa):
    ahora = time.time()
    tiempos_etapas.append((etapa, round(ahora - _marca[0], 1)))
    _marca[0] = ahora


# COMMAND ----------

# MAGIC %run ./00_ingesta/00_ingesta

# COMMAND ----------

marca("00_ingesta")

# COMMAND ----------

# MAGIC %run ./01_limpieza/01_limpieza

# COMMAND ----------

marca("01_limpieza")

# COMMAND ----------

# MAGIC %run ./03_seguridad/03_seguridad

# COMMAND ----------

marca("03_seguridad")

# COMMAND ----------

# MAGIC %run ./02_analisis/02_analisis

# COMMAND ----------

marca("02_analisis")

# COMMAND ----------

# MAGIC %run ./02_analisis/02b_modelo

# COMMAND ----------

marca("02b_modelo")

# COMMAND ----------

# MAGIC %run ./02_analisis/02c_optimizacion

# COMMAND ----------

marca("02c_optimizacion")

# COMMAND ----------

# MAGIC %run ./04_visualizacion/04_visualizacion

# COMMAND ----------

marca("04_visualizacion")

# COMMAND ----------

# MAGIC %run ./04_visualizacion/04b_integracion

# COMMAND ----------

marca("04b_integracion")

# COMMAND ----------

# MAGIC %run ./04_visualizacion/04c_monitoreo

# COMMAND ----------

marca("04c_monitoreo")
display(spark.createDataFrame(tiempos_etapas, ["etapa", "segundos"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen de resultados
# MAGIC Reúne en una sola salida las cifras que usan el documento y las diapositivas.

# COMMAND ----------

import json


def filas_de(nombre):
    return [r.asDict() for r in spark.table(f"{ESQUEMA}.{nombre}").collect()]


RESUMEN = {"perfil": perfil, "bitacora": bitacora, "riesgo": filas_riesgo, "etapas": tiempos_etapas,
           "tasa": filas_de("oro_tasa_localidad"), "clase_franja": filas_de("oro_clase_franja"), "franja": filas_de("oro_franja"),
           "modelo": filas_de("oro_modelo"), "optimizacion": filas_de("oro_optimizacion"), "monitoreo": filas_de("monitoreo_ejecuciones")}
print("RESULTADOS_JSON=" + json.dumps(RESUMEN, ensure_ascii=False, default=str))
