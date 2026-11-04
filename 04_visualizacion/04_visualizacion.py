# Databricks notebook source
# MAGIC %md
# MAGIC # 04_visualizacion · Capa oro y tablero
# MAGIC Seis visualizaciones, cada una con título, unidad y lectura. Las celdas de menos de 5 casos se ocultan. El tablero nativo está en `04_visualizacion/tablero_siniestralidad.lvdash.json`.

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np

ESQUEMA = "workspace.default"
AZUL, ROJO, GRIS = "#2a5db0", "#b4432d", "#6b7280"
plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False, "axes.edgecolor": GRIS,
                     "axes.titleweight": "bold", "axes.titlesize": 11, "figure.dpi": 110})


def tabla(nombre):
    return spark.table(f"{ESQUEMA}.{nombre}")


def num(v, d=0):
    return f"{v:,.{d}f}".replace(",", "#").replace(".", ",").replace("#", ".")


p_hora = tabla("oro_hora").orderBy("hora").toPandas()
p_loc = tabla("oro_localidad").filter("localidad != 'SIN INFORMACION'").orderBy("con_victimas").toPandas()
p_tasa = tabla("oro_tasa_localidad").filter("localidad != 'SUMAPAZ'").orderBy("tasa_victimas").toPandas()
p_clase = tabla("oro_clase").filter("siniestros >= 100").orderBy("pct_muertos").toPandas()
p_anio = tabla("oro_anio").orderBy("anio").toPandas()
p_cruce = tabla("oro_localidad_franja").filter("localidad != 'SIN INFORMACION'").toPandas()
cruce = p_cruce.pivot(index="localidad", columns="franja", values="con_victimas").reindex(p_loc["localidad"][::-1])
cruce = cruce.where(p_cruce.pivot(index="localidad", columns="franja", values="siniestros").reindex(cruce.index) >= 5)
cruce = cruce.where(cruce >= 5)


def g_hora(ax):
    ax.bar(p_hora["hora"], p_hora["con_victimas"], color=AZUL, width=0.8)
    ax.set_title("1. Siniestros con víctimas por hora, 2015-2021")
    ax.set_xlabel("Hora del día")
    ax.set_ylabel("Siniestros")
    ax.set_xticks(range(0, 24, 2))


def g_grave(ax):
    ax.plot(p_hora["hora"], p_hora["pct_muertos"], color=ROJO, linewidth=2, marker="o", markersize=4)
    ax.set_title("2. Siniestros con muertos por hora")
    ax.set_xlabel("Hora del día")
    ax.set_ylabel("% de los siniestros de esa hora")
    ax.set_xticks(range(0, 24, 2))
    ax.set_ylim(bottom=0)


def g_loc(ax):
    ax.barh(p_loc["localidad"], p_loc["con_victimas"], color=AZUL, height=0.7)
    ax.set_title("3. Siniestros con víctimas por localidad")
    ax.set_xlabel("Siniestros")
    ax.tick_params(axis="y", labelsize=8)
    for y, v in enumerate(p_loc["con_victimas"]):
        ax.text(v, y, " " + num(v), va="center", fontsize=8)
    ax.set_xlim(right=p_loc["con_victimas"].max() * 1.15)


def g_tasa(ax):
    ax.barh(p_tasa["localidad"], p_tasa["tasa_victimas"], color=AZUL, height=0.7)
    ax.set_title("4. Tasa de siniestros con víctimas, 2015-2020")
    ax.set_xlabel("Por 100.000 habitantes al año")
    ax.tick_params(axis="y", labelsize=8)
    for y, v in enumerate(p_tasa["tasa_victimas"]):
        ax.text(v, y, " " + num(v, 1), va="center", fontsize=8)
    ax.set_xlim(right=p_tasa["tasa_victimas"].max() * 1.15)


def g_cruce(ax):
    d = cruce.values.astype(float)
    mapa = plt.cm.Blues.copy()
    mapa.set_bad("#eeeeee")
    ax.imshow(np.ma.masked_invalid(d), cmap=mapa, aspect="auto")
    ax.set_xticks(range(len(cruce.columns)))
    ax.set_xticklabels([c[2:] for c in cruce.columns])
    ax.set_yticks(range(len(cruce.index)))
    ax.set_yticklabels(cruce.index, fontsize=8)
    ax.set_title("5. Siniestros con víctimas por localidad y franja")
    corte = np.nanmax(d) * 0.55
    for i in range(d.shape[0]):
        for j in range(d.shape[1]):
            v = d[i, j]
            ax.text(j, i, "<5" if np.isnan(v) else num(v), ha="center", va="center", fontsize=7.5,
                    color="white" if (not np.isnan(v) and v > corte) else "#222222")
    for s in ax.spines.values():
        s.set_visible(False)


def g_clase(ax):
    ax.barh(p_clase["clase"], p_clase["pct_muertos"], color=ROJO, height=0.6)
    ax.set_title("6. Siniestros con muertos por clase")
    ax.set_xlabel("% de los siniestros de esa clase")
    for y, (v, n) in enumerate(zip(p_clase["pct_muertos"], p_clase["siniestros"])):
        ax.text(v, y, f" {num(v,1)} % (n = {num(n)})", va="center", fontsize=8)
    ax.set_xlim(right=p_clase["pct_muertos"].max() * 1.5)


def mostrar(fn, w=10, h=4.5):
    fig, ax = plt.subplots(figsize=(w, h))
    fn(ax)
    plt.tight_layout()
    plt.show()


# COMMAND ----------

mostrar(g_hora)
print("Lectura: los siniestros con víctimas tienen dos picos, a primera hora de la mañana y al final de la tarde.")
print(f"La hora más alta es la {int(p_hora.loc[p_hora.con_victimas.idxmax(),'hora'])}, con {num(p_hora.con_victimas.max())}; ahí conviene concentrar el refuerzo diurno.")

# COMMAND ----------

mostrar(g_grave)
print(f"Lectura: la gravedad va al revés del volumen: llega a {num(p_hora.pct_muertos.max(),2)} % en la hora {int(p_hora.loc[p_hora.pct_muertos.idxmax(),'hora'])} y baja a {num(p_hora.pct_muertos.min(),2)} % en la hora {int(p_hora.loc[p_hora.pct_muertos.idxmin(),'hora'])}.")
print("La noche y la madrugada necesitan un control pensado para reducir gravedad.")

# COMMAND ----------

mostrar(g_loc, 10, 6.5)
print(f"Lectura: {p_loc.iloc[-1].localidad} lidera con {num(p_loc.iloc[-1].con_victimas)} siniestros con víctimas.")
print("Las localidades más pobladas y con mayor actividad vial aparecen arriba, por eso se complementa con la tasa.")

# COMMAND ----------

mostrar(g_tasa, 10, 6.5)
print(f"Lectura: por habitante, las más altas son {p_tasa.iloc[-1].localidad}, {p_tasa.iloc[-2].localidad} y {p_tasa.iloc[-3].localidad}. Sumapaz no se muestra por tener solo 7 registros.")
print("La población es la residente: una localidad de paso puede salir alta aunque los afectados vivan en otra parte.")

# COMMAND ----------

mostrar(g_cruce, 10, 7)
print("Lectura: el cruce responde dónde y cuándo a la vez, y permite ubicar el refuerzo por localidad y franja.")
print("Las celdas con menos de 5 casos se ocultan para no exponer hechos individuales.")

# COMMAND ----------

mostrar(g_clase)
print("Lectura: el atropello y el volcamiento tienen el mayor porcentaje con muertos, muy por encima del choque.")
print(f"Por su volumen de casos ({num(p_clase[p_clase.clase=='ATROPELLO'].siniestros.iloc[0])}), el atropello pesa más que el volcamiento para priorizar: protege al peatón.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tablero final

# COMMAND ----------

fig = plt.figure(figsize=(16, 15))
rejilla = fig.add_gridspec(3, 2)
for i, fn in enumerate([g_hora, g_grave, g_loc, g_tasa, g_cruce, g_clase]):
    fn(fig.add_subplot(rejilla[i // 2, i % 2]))
fig.suptitle("Siniestralidad vial en Bogotá, 2015-2021: tablero de priorización", fontsize=15, fontweight="bold")
plt.tight_layout()
plt.show()
