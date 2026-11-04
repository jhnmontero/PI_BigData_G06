# Databricks notebook source
# MAGIC %md
# MAGIC # 04b_integracion · Dos destinos y contrato de datos
# MAGIC - **Destino 1, archivos:** Parquet y CSV en un Volume de Unity Catalog, para sistemas que leen archivos.
# MAGIC - **Destino 2, consumo por SQL:** una vista que una herramienta de reportes puede leer con JDBC por medio de un SQL Warehouse. Queda lista, pero no se conectó a una herramienta externa.
# MAGIC
# MAGIC También se genera la muestra anonimizada del paquete de entrega.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

ESQUEMA = "workspace.default"
RUTA = "/Volumes/workspace/default/exportes"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {ESQUEMA}.exportes")
publicable = spark.table(f"{ESQUEMA}.oro_localidad_franja").filter("publicable AND localidad != 'SIN INFORMACION'").drop("publicable")

# COMMAND ----------

publicable.coalesce(1).write.mode("overwrite").parquet(f"{RUTA}/priorizacion_parquet")
publicable.toPandas().to_csv(f"{RUTA}/priorizacion.csv", index=False)
spark.sql(f"CREATE OR REPLACE VIEW {ESQUEMA}.vw_priorizacion AS SELECT * FROM {ESQUEMA}.oro_localidad_franja WHERE publicable AND localidad != 'SIN INFORMACION'")
print("Filas en Parquet:", spark.read.parquet(f"{RUTA}/priorizacion_parquet").count())
print("Filas en la vista:", spark.table(f"{ESQUEMA}.vw_priorizacion").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contrato de datos

# COMMAND ----------

CONTRATO = [("localidad", "string", "Sí", "Por corte de datos", "Ingeniería de datos"),
            ("franja", "string", "Sí", "Por corte de datos", "Ingeniería de datos"),
            ("siniestros", "bigint", "Sí", "Por corte de datos", "Ingeniería de datos"),
            ("con_victimas", "bigint", "Sí", "Por corte de datos", "Ingeniería de datos"),
            ("con_muertos", "bigint", "Sí", "Por corte de datos", "Ingeniería de datos"),
            ("pct_victimas", "double", "Sí", "Por corte de datos", "Ingeniería de datos"),
            ("pct_muertos", "double", "Sí", "Por corte de datos", "Ingeniería de datos"),
            ("puesto_en_franja", "int", "Sí", "Por corte de datos", "Ingeniería de datos")]
display(spark.createDataFrame(CONTRATO, ["campo", "tipo", "obligatorio", "frecuencia", "responsable"]))
print("Tipos reales:", publicable.dtypes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Muestra anonimizada
# MAGIC Solo año, franja, localidad, clase y gravedad, y solo combinaciones con 5 casos o más. Se conservan como máximo 5.000 filas.

# COMMAND ----------

COL = ["anio", "franja", "localidad", "clase", "gravedad"]
base = spark.table(f"{ESQUEMA}.siniestros_anonima").select(*COL)
vent = Window.partitionBy(*COL)
segura = base.withColumn("k", F.count("*").over(vent)).filter("k >= 5").drop("k")
muestra = segura.sample(fraction=min(1.0, 6000 / segura.count()), seed=6).limit(5000).toPandas()
muestra.to_csv(f"{RUTA}/muestra_anonimizada.csv", index=False)
print("Filas de la muestra:", len(muestra))
print("Descarga la muestra desde Catalog > workspace > default > exportes > muestra_anonimizada.csv")
