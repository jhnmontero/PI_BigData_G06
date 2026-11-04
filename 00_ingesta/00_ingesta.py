# Databricks notebook source
# MAGIC %md
# MAGIC # 00_ingesta · Capa bronce
# MAGIC **Proyecto Integrador · Grupo 06 · Movilidad y transporte**
# MAGIC
# MAGIC Lee las dos fuentes oficiales, las deja como tablas Delta de capa bronce y valida conteo, esquema y nulos.
# MAGIC
# MAGIC | Fuente | Publica | URL | Descarga |
# MAGIC |---|---|---|---|
# MAGIC | Histórico Siniestros Bogotá D.C. | Secretaría Distrital de Movilidad | https://datosabiertos.bogota.gov.co/dataset/historico-siniestros-bogota-d-c (el 3-nov-2026 esa dirección ya no abre) | 9-sep-2026 |
# MAGIC | Proyecciones y retroproyecciones de población por localidad 2005-2035 | Datos Abiertos Bogotá (autor en metadatos: Dirección de Información y Estadísticas) | https://datosabiertos.bogota.gov.co/dataset/0f1fb76a-1595-43d5-9257-0abcc3db908a/resource/5669484f-dc1d-4457-b1d1-de0a3825b41c/download/202503_localidad_proyeccion_retroproyeccion_poblacion_2005_2035.ods | 3-nov-2026 |

# COMMAND ----------

from pyspark.sql import functions as F

ESQUEMA = "workspace.default"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fuente 1: siniestros
# MAGIC El CSV completo se cargó como tabla Delta administrada. Esa tabla no se modifica: es el dato original.
# MAGIC
# MAGIC **Validación 1: conteo de filas y columnas.**

# COMMAND ----------

bronce = spark.table(f"{ESQUEMA}.siniestros_bogota")
print("Filas:", bronce.count())
print("Columnas:", len(bronce.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC **Validación 2: esquema.** Las fechas vienen como texto; se convierten en `01_limpieza`.

# COMMAND ----------

bronce.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Validación 3: nulos por columna.**

# COMMAND ----------

total = bronce.count()
nulos = bronce.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in bronce.columns]).first().asDict()
display(spark.createDataFrame([(c, int(n), round(100 * n / total, 3)) for c, n in nulos.items()], ["columna", "nulos", "pct_nulos"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fuente 2: población por localidad
# MAGIC Se extrajo el total de cada localidad entre 2015 y 2021, sumando cabecera y área rural del archivo oficial `.ods`. Se escribe aquí la extracción porque son 140 datos y así el notebook corre sin archivos externos. Los nombres quedan en mayúsculas y sin tildes para cruzar con los siniestros.

# COMMAND ----------

POBLACION = {
    "ANTONIO NARINO": [81014, 80014, 79336, 79229, 80095, 81356, 81238],
    "BARRIOS UNIDOS": [138250, 135739, 133890, 133126, 138316, 140237, 140987],
    "BOSA": [671404, 677908, 684570, 694397, 707173, 722905, 735259],
    "CANDELARIA": [17290, 17141, 17049, 17075, 17345, 17508, 17361],
    "CHAPINERO": [151996, 153204, 154506, 156479, 163148, 165952, 166903],
    "CIUDAD BOLIVAR": [601716, 603882, 606799, 613127, 628670, 642989, 652533],
    "ENGATIVA": [783433, 784129, 786139, 792518, 802780, 816160, 824322],
    "FONTIBON": [356388, 358804, 361474, 365884, 377118, 383577, 388044],
    "KENNEDY": [1001537, 1004979, 1009665, 1019748, 1027373, 1046951, 1061182],
    "LOS MARTIRES": [73987, 73440, 73112, 73277, 82957, 84302, 84690],
    "PUENTE ARANDA": [240697, 240673, 241114, 242905, 247237, 250855, 252241],
    "RAFAEL URIBE URIBE": [360924, 361010, 361722, 364532, 372981, 379794, 381878],
    "SAN CRISTOBAL": [382247, 382102, 382696, 385514, 392322, 399766, 401341],
    "SANTA FE": [102155, 102485, 102984, 103985, 105926, 107824, 109831],
    "SUBA": [1120332, 1128753, 1137868, 1152387, 1192644, 1213498, 1230255],
    "SUMAPAZ": [3224, 3166, 3137, 3138, 3298, 3383, 3450],
    "TEUSAQUILLO": [142245, 142633, 143211, 144526, 152414, 154339, 156451],
    "TUNJUELITO": [170388, 170248, 170440, 171632, 175481, 178491, 179052],
    "USAQUEN": [520480, 524563, 528924, 535693, 550706, 558873, 567530],
    "USME": [353558, 356045, 358813, 363394, 374887, 383401, 388786],
}
filas = [(loc, 2015 + i, v) for loc, vals in POBLACION.items() for i, v in enumerate(vals)]
poblacion = spark.createDataFrame(filas, ["localidad", "anio", "poblacion"])
poblacion.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{ESQUEMA}.poblacion_bronce")
print("Filas población:", poblacion.count(), "| Localidades:", poblacion.select("localidad").distinct().count())
display(poblacion.groupBy("anio").agg(F.sum("poblacion").alias("poblacion_bogota")).orderBy("anio"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Perfilado de columnas

# COMMAND ----------

COLUMNAS = ["CODIGO_ACCIDENTE", "FECHA_OCURRENCIA_ACC", "FECHA_HORA_ACC", "ANO_OCURRENCIA_ACC", "DIRECCION", "GRAVEDAD",
            "CLASE_ACC", "LOCALIDAD", "LATITUD", "LONGITUD", "CIV", "PK_CALZADA"]
tipos = dict(bronce.dtypes)
perfil = []
for c in COLUMNAS:
    r = bronce.agg(F.sum(F.col(c).isNull().cast("int")).alias("n"), F.countDistinct(c).alias("u"), F.min(c).alias("mn"), F.max(c).alias("mx")).first()
    perfil.append((c, tipos[c], round(100 * r["n"] / total, 3), int(r["u"]), str(r["mn"]), str(r["mx"])))
display(spark.createDataFrame(perfil, ["columna", "tipo", "pct_nulos", "valores_unicos", "minimo", "maximo"]))
