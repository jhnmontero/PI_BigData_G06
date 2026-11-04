# Databricks notebook source
# MAGIC %md
# MAGIC # 02c_optimizacion · Dos técnicas con tiempos antes y después
# MAGIC Cada consulta se mide 3 veces y se reporta la mediana. Los tiempos cambian entre corridas, y Databricks puede reutilizar resultados en caché. Con solo 199.146 filas puede no haber mejora: se reporta lo que salga.

# COMMAND ----------

import statistics
import time
from pyspark.sql import functions as F

ESQUEMA = "workspace.default"
plata = spark.table(f"{ESQUEMA}.siniestros_plata")
poblacion = spark.table(f"{ESQUEMA}.poblacion_bronce")
PRUEBA = f"{ESQUEMA}.opt_prueba"
FILTRO = "anio = 2019 AND localidad = 'KENNEDY'"
tiempos = []


def medir(fn):
    marcas = []
    for _ in range(3):
        t = time.perf_counter()
        fn()
        marcas.append(time.perf_counter() - t)
    return round(statistics.median(marcas), 3)


def consulta():
    return spark.table(PRUEBA).filter(FILTRO).groupBy("franja").count().collect()


def archivos():
    return spark.sql(f"DESCRIBE DETAIL {PRUEBA}").first()["numFiles"]


# COMMAND ----------

# MAGIC %md
# MAGIC ## Técnica 1: particionamiento por año + `OPTIMIZE ZORDER BY (localidad)`

# COMMAND ----------

plata.repartition(16).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(PRUEBA)
a_t, a_f = medir(consulta), archivos()
plata.repartition(16).write.mode("overwrite").option("overwriteSchema", "true").partitionBy("anio").saveAsTable(PRUEBA)
b_t, b_f = medir(consulta), archivos()
spark.sql(f"OPTIMIZE {PRUEBA} ZORDER BY (localidad)")
c_t, c_f = medir(consulta), archivos()
tiempos += [("Filtro por año y localidad", "Sin optimizar", a_t, a_f),
            ("Filtro por año y localidad", "Partición por año", b_t, b_f),
            ("Filtro por año y localidad", "Partición + OPTIMIZE ZORDER", c_t, c_f)]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Técnica 2: unión con `broadcast`
# MAGIC Se envía la tabla pequeña de población a todos los nodos. Databricks puede aplicarlo solo aunque no se pida.

# COMMAND ----------


def union(tabla):
    return plata.drop("poblacion").join(tabla, ["localidad", "anio"], "left").groupBy("localidad").agg(F.sum("poblacion")).collect()


tiempos += [("Unión siniestros y población", "Sin indicación", medir(lambda: union(poblacion)), None),
            ("Unión siniestros y población", "Con broadcast", medir(lambda: union(F.broadcast(poblacion))), None)]

# COMMAND ----------

resultado = spark.createDataFrame(tiempos, "consulta string, tecnica string, segundos double, archivos long")
resultado.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{ESQUEMA}.oro_optimizacion")
display(resultado)
spark.sql(f"DROP TABLE IF EXISTS {PRUEBA}")
print(f"Lectura: el filtro pasó de {a_t} s sin optimizar a {c_t} s con partición y ZORDER. La unión tardó {tiempos[3][2]} s sin indicación y {tiempos[4][2]} s con broadcast.")
print("Con este volumen, las diferencias pueden deberse a variación normal o a caché. La optimización se justifica cuando crezcan los datos.")
