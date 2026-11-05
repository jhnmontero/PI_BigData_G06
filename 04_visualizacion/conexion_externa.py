# Prueba de consumo externo: lee la vista vw_priorizacion desde fuera de Databricks, a través de un SQL Warehouse.
# Se ejecuta en un computador local, no en un notebook. Requiere: pip install databricks-sql-connector
# Variables de entorno: DATABRICKS_HOST, DATABRICKS_HTTP_PATH y DATABRICKS_TOKEN. La clave nunca va en el código.
import datetime
import os
import platform
import sys
from databricks import sql

SALIDA = sys.argv[1] if len(sys.argv) > 1 else "conexion_externa.txt"
VISTA = "workspace.default.vw_priorizacion"
lineas = [f"Prueba de conexión externa · {datetime.datetime.now().astimezone().isoformat(timespec='seconds')}",
          f"Cliente: Python {platform.python_version()} con databricks-sql-connector, en un computador local",
          "Destino: SQL Warehouse de Databricks", f"Vista consultada: {VISTA}", ""]
with sql.connect(server_hostname=os.environ["DATABRICKS_HOST"], http_path=os.environ["DATABRICKS_HTTP_PATH"], access_token=os.environ["DATABRICKS_TOKEN"]) as conexion:
    with conexion.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {VISTA}")
        lineas.append(f"Filas leídas: {cursor.fetchone()[0]}")
        cursor.execute(f"SELECT * FROM {VISTA} ORDER BY con_victimas DESC LIMIT 5")
        lineas.append("Columnas: " + ", ".join(c[0] for c in cursor.description))
        lineas.append("Cinco filas con más siniestros con víctimas:")
        lineas += [" | ".join(str(v) for v in fila) for fila in cursor.fetchall()]
texto = "\n".join(lineas) + "\n"
open(SALIDA, "w", encoding="utf-8").write(texto)
print(texto)
