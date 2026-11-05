# Databricks notebook source
# MAGIC %md
# MAGIC # 03_seguridad · Clasificación, anonimización y control de acceso
# MAGIC Marco: Ley 1581 de 2012 y Decreto 1377 de 2013. La ley define dato personal como cualquier información que pueda asociarse a una persona. La tabla no trae nombres, pero un siniestro concreto sí puede llevar a las personas involucradas.

# COMMAND ----------

from pyspark.sql import functions as F

ESQUEMA = "workspace.default"
plata = spark.table(f"{ESQUEMA}.siniestros_plata")

CLASIFICACION = [
    ("DIRECCION", "Alta", "Ubica el hecho exacto y puede asociarse a personas", "Se retira de la capa anónima"),
    ("LATITUD / LONGITUD / X / Y", "Alta", "Ubicación exacta", "Se generaliza a 2 decimales para la demostración y se retira de la capa anónima"),
    ("FECHA_HORA_ACC", "Alta", "Junto con la ubicación identifica un hecho único", "Se generaliza a franja horaria"),
    ("FORMULARIO / CODIGO_ACCIDENTE", "Media", "Permite ligar el registro con el informe del siniestro", "Hash SHA-256 con sal"),
    ("CIV / PK_CALZADA", "Media", "Ubican el tramo de vía", "Se retiran de la capa anónima"),
    ("LOCALIDAD", "Baja", "Zona amplia de la ciudad", "Se conserva"),
    ("GRAVEDAD / CLASE_ACC", "Baja", "Describen el hecho, no a la persona", "Se conservan"),
]
display(spark.createDataFrame(CLASIFICACION, ["columna", "sensibilidad", "razon", "tratamiento"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Riesgo de reidentificación
# MAGIC `k` es cuántos registros comparten la misma combinación. Con `k = 1`, ese registro queda identificado por esa combinación.

# COMMAND ----------


def riesgo(df, columnas):
    grupos = df.groupBy(*columnas).count()
    r = grupos.agg(F.count("*").alias("grupos"),
                   F.sum((F.col("count") == 1).cast("int")).alias("grupos_k1"),
                   F.sum((F.col("count") < 5).cast("int")).alias("grupos_k_menor_5"),
                   F.min("count").alias("k_minimo")).first()
    return (" + ".join(columnas), int(r["grupos"]), int(r["grupos_k1"]), int(r["grupos_k_menor_5"]), int(r["k_minimo"]))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Técnica 1: hash SHA-256. Técnica 2: generalización.
# MAGIC La sal es de demostración; en producción iría en un *secret scope*, nunca en el código. El antes y el después no muestra direcciones.

# COMMAND ----------

SAL = "G06-demostracion"
transformada = (plata
                .withColumn("id_hash", F.sha2(F.concat(F.lit(SAL), F.col("CODIGO_ACCIDENTE").cast("string")), 256))
                .withColumn("lat_2d", F.round("LATITUD", 2))
                .withColumn("lon_2d", F.round("LONGITUD", 2)))
display(transformada.select(F.col("CODIGO_ACCIDENTE").alias("antes_codigo"), F.col("id_hash").alias("despues_hash"),
                            F.col("fecha_hora").alias("antes_fecha_hora"), F.col("franja").alias("despues_franja"),
                            F.col("LATITUD").alias("antes_lat"), F.col("lat_2d").alias("despues_lat"),
                            F.col("LONGITUD").alias("antes_lon"), F.col("lon_2d").alias("despues_lon")).limit(3))

# COMMAND ----------

anonima = transformada.select("id_hash", "anio", "mes", "dia_semana", "franja", "hora", "localidad", "clase", "gravedad", "con_victimas", "con_muertos")
anonima.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{ESQUEMA}.siniestros_anonima")
anonima = spark.table(f"{ESQUEMA}.siniestros_anonima")
print("Filas capa anónima:", anonima.count(), "| Columnas:", anonima.columns)

filas_riesgo = [riesgo(plata, ["DIRECCION", "FECHA_HORA_ACC"]),
                riesgo(plata, ["LATITUD", "LONGITUD", "fecha"]),
                riesgo(transformada, ["lat_2d", "lon_2d", "franja", "anio", "mes"]),
                riesgo(anonima, ["anio", "mes", "dia_semana", "franja", "localidad", "clase", "gravedad"]),
                riesgo(anonima, ["anio", "franja", "localidad"]),
                riesgo(anonima, ["franja", "localidad"])]
display(spark.createDataFrame(filas_riesgo, ["atributos", "grupos", "grupos_k1", "grupos_k_menor_5", "k_minimo"]))
detallada = filas_riesgo[3]
print(f"Lectura: aun sin dirección ni coordenadas, {detallada[2]} de {detallada[1]} combinaciones detalladas son únicas.")
print("Por eso la capa anónima es de uso interno y a la Secretaría solo salen agregados oro, sin celdas de menos de 5 casos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control de acceso por rol
# MAGIC Esta es la política definida. El espacio de trabajo gratuito no tiene estos tres grupos, así que en una entidad real se aplicaría con las instrucciones de abajo. Lo que sí se puede aplicar aquí, con el grupo `account users`, se ejecuta al final de `04b_integracion`, porque las tablas oro y la vista se crean después de este notebook.
# MAGIC
# MAGIC | Rol | Bronce y plata | Anónima | Oro |
# MAGIC |---|---|---|---|
# MAGIC | Ingeniería de datos | Lectura y escritura | Lectura y escritura | Lectura y escritura |
# MAGIC | Analistas internos | Sin acceso | Lectura | Lectura |
# MAGIC | Secretaría y publicación | Sin acceso | Sin acceso | Lectura |
# MAGIC
# MAGIC ```sql
# MAGIC GRANT SELECT, MODIFY ON TABLE workspace.default.siniestros_plata TO `ingenieria_datos`;
# MAGIC GRANT SELECT ON TABLE workspace.default.siniestros_anonima TO `analistas_internos`;
# MAGIC GRANT SELECT ON TABLE workspace.default.oro_localidad TO `secretaria_movilidad`;
# MAGIC REVOKE SELECT ON TABLE workspace.default.siniestros_plata FROM `secretaria_movilidad`;
# MAGIC ```
