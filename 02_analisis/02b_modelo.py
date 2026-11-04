# Databricks notebook source
# MAGIC %md
# MAGIC # 02b_modelo · Modelo analítico
# MAGIC **Objetivo:** estimar si un siniestro deja víctimas según hora, día, mes, localidad y clase. Sirve para ver qué factores se asocian con víctimas, no para anticipar cuándo ocurrirá un siniestro.
# MAGIC
# MAGIC **Modelo:** regresión logística, porque sus coeficientes se pueden explicar.
# MAGIC
# MAGIC **Partición:** entrenamiento 2015-2019 y prueba 2020-2021. Es temporal para evaluar con años que el modelo no vio.
# MAGIC
# MAGIC **Línea base:** predecir siempre la clase más frecuente. Se compara con 2 métricas: F1 y AUC.

# COMMAND ----------

import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

ESQUEMA = "workspace.default"
COLS = ["hora", "dia_semana", "mes", "localidad", "clase"]
datos = spark.table(f"{ESQUEMA}.siniestros_anonima").select("anio", "con_victimas", *COLS).toPandas()
for c in ["hora", "mes"]:
    datos[c] = datos[c].astype(str)
ent, pru = datos[datos.anio <= 2019], datos[datos.anio >= 2020]
y_ent, y_pru = ent.con_victimas.astype(int), pru.con_victimas.astype(int)
print("Entrenamiento:", len(ent), "| Prueba:", len(pru), "| % con víctimas en prueba:", round(100 * y_pru.mean(), 1))

# COMMAND ----------


def evaluar(nombre, modelo, columnas):
    modelo.fit(ent[columnas], y_ent)
    pred = modelo.predict(pru[columnas])
    prob = modelo.predict_proba(pru[columnas])[:, 1]
    return (nombre, round(accuracy_score(y_pru, pred), 3), round(f1_score(y_pru, pred, zero_division=0), 3), round(roc_auc_score(y_pru, prob), 3))


def logistica(columnas):
    prep = ColumnTransformer([("cat", OneHotEncoder(handle_unknown="ignore"), columnas)])
    return Pipeline([("prep", prep), ("lr", LogisticRegression(max_iter=1000))])


SIN_CLASE = ["hora", "dia_semana", "mes", "localidad"]
final = logistica(COLS)
filas = [evaluar("Línea base: clase más frecuente", DummyClassifier(strategy="most_frequent"), COLS),
         evaluar("Logística sin clase de siniestro", logistica(SIN_CLASE), SIN_CLASE),
         evaluar("Logística completa", final, COLS)]
resultados = spark.createDataFrame(filas, ["modelo", "exactitud", "f1", "auc"])
resultados.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{ESQUEMA}.oro_modelo")
display(resultados)
b, s, m = filas
print(f"Lectura: la línea base logra exactitud {b[1]} y F1 {b[2]}, porque nunca predice víctimas. El modelo completo logra exactitud {m[1]}, F1 {m[2]} y AUC {m[3]}.")
print(f"Sin la clase de siniestro, el AUC es {s[3]}: con solo hora, día, mes y localidad se distingue mucho menos. El peso principal está en la clase, que solo se conoce después del hecho.")

# COMMAND ----------

nombres = final.named_steps["prep"].get_feature_names_out()
coef = final.named_steps["lr"].coef_[0]
orden = np.argsort(coef)
tabla = [(nombres[i].replace("cat__", ""), round(float(coef[i]), 3)) for i in list(orden[-10:][::-1]) + list(orden[:10])]
display(spark.createDataFrame(tabla, ["factor", "coeficiente"]))
print("Lectura: coeficiente positivo significa mayor probabilidad de víctimas, frente a la categoría promedio y con lo demás igual.")
print("Es una asociación estadística, no una causa.")
