# Databricks notebook source
# MAGIC %md
# MAGIC # 02_analisis · Preguntas de negocio y capa oro
# MAGIC
# MAGIC | Pregunta | KPI | Fórmula | Unidad | Fuente |
# MAGIC |---|---|---|---|---|
# MAGIC | P1. ¿Cuánto de la siniestralidad deja víctimas? | KPI 1: siniestros con víctimas | con víctimas / siniestros × 100 | % | GRAVEDAD |
# MAGIC | P2. ¿Dónde y en qué horarios se concentran los siniestros con víctimas? | KPI 2: tasa poblacional | con víctimas / población-año × 100.000 | por 100.000 hab. al año | GRAVEDAD, LOCALIDAD y población |
# MAGIC | P3. ¿Por qué unos siniestros son más graves? | KPI 3: severidad | con muertos / siniestros × 100 | % | GRAVEDAD, CLASE_ACC y hora |
# MAGIC
# MAGIC P3 busca factores **asociados**. Estos datos no prueban causas.

# COMMAND ----------

from pyspark.sql import functions as F

ESQUEMA = "workspace.default"
spark.table(f"{ESQUEMA}.siniestros_anonima").createOrReplaceTempView("anonima")
spark.table(f"{ESQUEMA}.poblacion_bronce").createOrReplaceTempView("poblacion")


def num(v, d=0):
    return f"{v:,.{d}f}".replace(",", "#").replace(".", ",").replace("#", ".")


def guardar(sql, nombre):
    spark.sql(sql).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{ESQUEMA}.{nombre}")
    return spark.table(f"{ESQUEMA}.{nombre}")


METRICAS = """COUNT(*) AS siniestros,
SUM(CAST(con_victimas AS INT)) AS con_victimas,
SUM(CAST(con_muertos AS INT)) AS con_muertos,
ROUND(100 * SUM(CAST(con_victimas AS INT)) / COUNT(*), 1) AS pct_victimas,
ROUND(100 * SUM(CAST(con_muertos AS INT)) / COUNT(*), 2) AS pct_muertos"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consulta 1 (P1): ¿cuánto y cómo evoluciona?

# COMMAND ----------

oro_anio = guardar(f"SELECT anio, {METRICAS} FROM anonima GROUP BY anio", "oro_anio")
display(oro_anio.orderBy("anio"))
t = spark.sql(f"SELECT {METRICAS} FROM anonima").first()
a = {r["anio"]: r for r in oro_anio.collect()}
print(f"Lectura: {num(t['con_victimas'])} de {num(t['siniestros'])} siniestros dejaron víctimas ({num(t['pct_victimas'],1)} %) y {num(t['con_muertos'])} dejaron muertos ({num(t['pct_muertos'],2)} %).")
print(f"Los siniestros bajan de {num(a[2019]['siniestros'])} en 2019 a {num(a[2020]['siniestros'])} en 2020, y el registro de 2021 está incompleto.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consulta 2 (P2): ¿en qué horas? La misma tabla comprueba si la hora con `+00` es hora de Bogotá.

# COMMAND ----------

oro_hora = guardar(f"SELECT hora, {METRICAS} FROM anonima GROUP BY hora", "oro_hora")
display(oro_hora.orderBy("hora"))
h = {r["hora"]: r for r in oro_hora.collect()}
pico_m = h[6]["con_victimas"] + h[7]["con_victimas"]
pico_t = h[17]["con_victimas"] + h[18]["con_victimas"] + h[19]["con_victimas"]
mas_letal = max(h.values(), key=lambda r: r["pct_muertos"])
minimo = min(h.values(), key=lambda r: r["siniestros"])
print(f"Lectura: los picos con víctimas son de 6 a 7 a. m. ({num(pico_m)}) y de 5 a 7 p. m. ({num(pico_t)}). La hora más grave es la {mas_letal['hora']}, con {num(mas_letal['pct_muertos'],2)} % con muertos.")
print(f"El mínimo de siniestros está a la hora {minimo['hora']}, lo que corresponde a madrugada en Bogotá: la marca +00 no es una hora corrida.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consulta 3 (P2): ¿en qué localidades? Con función de ventana para la participación acumulada.

# COMMAND ----------

oro_localidad = guardar(f"""
WITH base AS (SELECT localidad, {METRICAS} FROM anonima GROUP BY localidad)
SELECT *,
  RANK() OVER (ORDER BY con_victimas DESC) AS puesto_victimas,
  RANK() OVER (ORDER BY con_muertos DESC) AS puesto_muertos,
  RANK() OVER (ORDER BY siniestros DESC) AS puesto_total,
  ROUND(100 * SUM(con_victimas) OVER (ORDER BY con_victimas DESC) / SUM(con_victimas) OVER (), 1) AS pct_acumulado
FROM base""", "oro_localidad")
display(oro_localidad.orderBy("puesto_victimas"))
L = oro_localidad.filter("localidad != 'SIN INFORMACION'").orderBy("puesto_victimas").collect()
print(f"Lectura: {L[0]['localidad']} es primera, con {num(L[0]['con_victimas'])} siniestros con víctimas. Las cinco primeras ({', '.join(r['localidad'] for r in L[:5])}) acumulan el {num(L[4]['pct_acumulado'],1)} %.")
print("Comparar los tres puestos muestra que el orden cambia según lo que se cuente.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consulta 4 (P2): tasa por población. Unión entre las dos fuentes.
# MAGIC Se usan los años completos 2015 a 2020. La población es la residente: una localidad con mucho tránsito de paso puede salir alta sin que su gente sea la afectada.

# COMMAND ----------

oro_tasa = guardar("""
WITH s AS (SELECT localidad, SUM(CAST(con_victimas AS INT)) AS con_victimas, SUM(CAST(con_muertos AS INT)) AS con_muertos
           FROM anonima WHERE anio BETWEEN 2015 AND 2020 GROUP BY localidad),
     p AS (SELECT localidad, SUM(poblacion) AS poblacion_anios FROM poblacion WHERE anio BETWEEN 2015 AND 2020 GROUP BY localidad)
SELECT s.localidad, s.con_victimas, s.con_muertos, p.poblacion_anios,
       ROUND(100000 * s.con_victimas / p.poblacion_anios, 1) AS tasa_victimas,
       ROUND(100000 * s.con_muertos / p.poblacion_anios, 2) AS tasa_muertos,
       RANK() OVER (ORDER BY s.con_victimas / p.poblacion_anios DESC) AS puesto_tasa
FROM s JOIN p ON s.localidad = p.localidad""", "oro_tasa_localidad")
display(oro_tasa.orderBy("puesto_tasa"))
T = oro_tasa.orderBy("puesto_tasa").collect()
ciudad = oro_tasa.agg(F.sum("con_victimas").alias("v"), F.sum("poblacion_anios").alias("p")).first()
print(f"Lectura: la tasa de Bogotá es {num(100000*ciudad['v']/ciudad['p'],1)} siniestros con víctimas por 100.000 habitantes al año. Las más altas son {', '.join(r['localidad'] for r in T[:3])}.")
print("Es una pista distinta al total: localidades pequeñas de paso quedan arriba aunque tengan menos casos. Por eso la tasa complementa el total y no lo reemplaza.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consulta 5 (P2): localidad y franja juntas, con puesto dentro de cada franja.

# COMMAND ----------

oro_cruce = guardar(f"""
WITH base AS (SELECT localidad, franja, {METRICAS} FROM anonima GROUP BY localidad, franja)
SELECT *, siniestros >= 5 AS publicable,
       RANK() OVER (PARTITION BY franja ORDER BY con_victimas DESC) AS puesto_en_franja
FROM base""", "oro_localidad_franja")
vista = oro_cruce.filter("publicable AND localidad != 'SIN INFORMACION'")
display(vista.orderBy(F.desc("con_victimas")).limit(15))
display(vista.orderBy(F.desc("con_muertos")).limit(15))
primeras = {r["franja"]: r["localidad"] for r in vista.filter("puesto_en_franja = 1").collect()}
top_m = vista.orderBy(F.desc("con_muertos")).first()
print(f"Lectura: primera localidad por franja: {primeras}.")
print(f"La celda con más siniestros con muertos es {top_m['localidad']} en {top_m['franja'][2:]}, con {num(top_m['con_muertos'])}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consulta 6 (P3): ¿qué se asocia con mayor gravedad?

# COMMAND ----------

oro_clase = guardar(f"SELECT clase, {METRICAS} FROM anonima GROUP BY clase", "oro_clase")
oro_franja = guardar(f"SELECT franja, {METRICAS} FROM anonima GROUP BY franja", "oro_franja")
oro_clase_franja = guardar(f"SELECT clase, franja, {METRICAS} FROM anonima WHERE clase IN ('ATROPELLO','CHOQUE') GROUP BY clase, franja", "oro_clase_franja")
display(oro_clase.orderBy(F.desc("siniestros")))
display(oro_franja.orderBy("franja"))
display(oro_clase_franja.orderBy("clase", "franja"))
c = {r["clase"]: r for r in oro_clase.collect()}
f = {r["franja"]: r for r in oro_franja.collect()}
print(f"Lectura: el atropello deja {num(c['ATROPELLO']['pct_muertos'],2)} % con muertos y el choque {num(c['CHOQUE']['pct_muertos'],2)} %. Los choques son {num(c['CHOQUE']['siniestros']/c['ATROPELLO']['siniestros'],1)} veces más, pero los siniestros con muertos son casi iguales: {num(c['ATROPELLO']['con_muertos'])} y {num(c['CHOQUE']['con_muertos'])}.")
print(f"Por franja, la madrugada tiene {num(f['1 Madrugada (0-5)']['pct_muertos'],2)} % con muertos frente a {num(f['3 Tarde (12-17)']['pct_muertos'],2)} % en la tarde. Es asociación: los datos no explican la causa.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consulta 7: día de la semana y franja

# COMMAND ----------

oro_dia = guardar(f"SELECT dia_semana, franja, {METRICAS} FROM anonima GROUP BY dia_semana, franja", "oro_dia_franja")
display(oro_dia.orderBy(F.desc("con_victimas")).limit(10))
d = oro_dia.orderBy(F.desc("con_victimas")).first()
print(f"Lectura: la combinación con más siniestros con víctimas es {d['dia_semana']} en {d['franja'][2:]}, con {num(d['con_victimas'])}.")
print("Sirve para decidir en qué días programar el refuerzo de esa franja.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba de solidez: sin los años de cuarentena

# COMMAND ----------

normal = spark.sql(f"SELECT localidad, {METRICAS} FROM anonima WHERE anio <= 2019 AND localidad != 'SIN INFORMACION' GROUP BY localidad ORDER BY con_victimas DESC LIMIT 10")
display(normal)
top_n = [r["localidad"] for r in normal.collect()]
print("Lectura: cinco primeras con todos los años:", [r["localidad"] for r in L[:5]])
print("Cinco primeras con 2015-2019:", top_n[:5], "| Iguales:", [r["localidad"] for r in L[:5]] == top_n[:5])
