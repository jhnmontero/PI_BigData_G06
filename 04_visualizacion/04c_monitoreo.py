# Databricks notebook source
# MAGIC %md
# MAGIC # 04c_monitoreo · Métricas de operación y alerta
# MAGIC Cada corrida agrega una fila a `monitoreo_ejecuciones`.
# MAGIC
# MAGIC **Métricas:** tiempo de ejecución, filas procesadas, % de nulos en `LOCALIDAD` y consumo de recursos. Como indicador de recursos se usa el almacenamiento de las tablas, porque el costo de cómputo no se mide aquí.
# MAGIC
# MAGIC **Regla de alerta:** se dispara si las filas son menos de 199.146, si los nulos de `LOCALIDAD` superan 0,1 % o si el tiempo supera el doble de la mediana anterior.

# COMMAND ----------

import datetime
import statistics
import time
import matplotlib.pyplot as plt
from pyspark.sql import functions as F

ESQUEMA = "workspace.default"
TABLA = f"{ESQUEMA}.monitoreo_ejecuciones"
FILAS_ESPERADAS = 199146
inicio = globals().get("INICIO_EJECUCION", time.time())

bronce = spark.table(f"{ESQUEMA}.siniestros_bogota")
plata = spark.table(f"{ESQUEMA}.siniestros_plata")
filas = plata.count()
nulos = round(100 * bronce.filter(F.col("LOCALIDAD").isNull()).count() / bronce.count(), 4)
TABLAS = ["siniestros_bogota", "siniestros_plata", "siniestros_anonima", "oro_hora", "oro_localidad", "oro_localidad_franja"]
tamano = sum(spark.sql(f"DESCRIBE DETAIL {ESQUEMA}.{t}").first()["sizeInBytes"] for t in TABLAS) / 1048576
duracion = round(time.time() - inicio, 1)
fila = [(datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"), float(duracion), int(filas), float(nulos), round(float(tamano), 3))]
esquema = "ejecucion string, segundos double, filas long, pct_nulos_localidad double, almacenamiento_mib double"

# COMMAND ----------

anteriores = []
if spark.catalog.tableExists(TABLA):
    anteriores = [r["segundos"] for r in spark.table(TABLA).select("segundos").collect()]
spark.createDataFrame(fila, esquema).write.mode("append").saveAsTable(TABLA)
historico = spark.table(TABLA).orderBy("ejecucion")
display(historico)

alertas = []
if filas < FILAS_ESPERADAS:
    alertas.append(f"Filas {filas} por debajo de {FILAS_ESPERADAS}")
if nulos > 0.1:
    alertas.append(f"Nulos de LOCALIDAD en {nulos} %, por encima de 0,1 %")
if anteriores and duracion > 2 * statistics.median(anteriores):
    alertas.append(f"Duración de {duracion} s, más del doble de la mediana anterior")
print("ALERTA: " + " | ".join(alertas) if alertas else "Sin alertas: los tres umbrales están dentro de lo esperado.")
print("Número de ejecuciones registradas:", historico.count())

# COMMAND ----------

p = historico.toPandas()
fig, ejes = plt.subplots(1, 4, figsize=(15, 3))
for ax, (col, titulo) in zip(ejes, [("segundos", "Tiempo (s)"), ("filas", "Filas"), ("pct_nulos_localidad", "% nulos LOCALIDAD"), ("almacenamiento_mib", "Almacenamiento (MiB)")]):
    ax.plot(range(1, len(p) + 1), p[col], color="#2a5db0", marker="o", linewidth=2)
    ax.set_title(titulo, fontsize=10, fontweight="bold")
    ax.set_xticks(range(1, len(p) + 1))
    ax.set_xlabel("Ejecución")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
plt.tight_layout()
plt.show()
