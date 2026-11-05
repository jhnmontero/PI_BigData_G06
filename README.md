# Proyecto Integrador de Big Data · Grupo 06 · Movilidad y transporte

Jhon Albert Montero Pinzón · Henry López Navarro
Docente: Ing. Jacksson Sonny González Bayona · Uniciencia · 2026-II

Análisis de la siniestralidad vial en Bogotá para priorizar dónde y cuándo reforzar el control, cuidando el anonimato.

## Preguntas de negocio
1. ¿Cuánto de la siniestralidad deja víctimas?
2. ¿Dónde y en qué horarios se concentran los siniestros con víctimas?
3. ¿Por qué unos siniestros son más graves? (factores asociados, no causas probadas)

## Estructura
| Carpeta | Contenido |
|---|---|
| `00_ingesta` | Capa bronce: siniestros y población; validaciones y perfilado |
| `01_limpieza` | Capa plata: 6 transformaciones con filas antes y después |
| `02_analisis` | Consultas y capa oro · `02b_modelo` · `02c_optimizacion` |
| `03_seguridad` | Clasificación, SHA-256, generalización, k y control de acceso |
| `04_visualizacion` | Tablero · `04b_integracion` · `04c_monitoreo` |
| `99_evidencias` | Capturas y archivos de evidencia |
| `ejecutar_todo` | Corre todo el flujo y es el notebook del trabajo programado |

## Fuentes
| Fuente | Editor | Dirección | Descarga |
|---|---|---|---|
| Histórico Siniestros Bogotá D.C. | Secretaría Distrital de Movilidad | https://datosabiertos.bogota.gov.co/dataset/historico-siniestros-bogota-d-c (el 3-nov-2026 esa dirección ya no abre) | 9-sep-2026 |
| Proyecciones y retroproyecciones de población por localidad 2005-2035 | Datos Abiertos Bogotá (autor en metadatos: Dirección de Información y Estadísticas) | https://datosabiertos.bogota.gov.co/dataset/0f1fb76a-1595-43d5-9257-0abcc3db908a/resource/5669484f-dc1d-4457-b1d1-de0a3825b41c/download/202503_localidad_proyeccion_retroproyeccion_poblacion_2005_2035.ods | 3-nov-2026 |

## Para reproducir
1. Crear una cuenta en Databricks Free Edition y cargar el CSV de siniestros como `workspace.default.siniestros_bogota`.
2. Traer este repositorio con **Create → Git folder**.
3. Abrir `ejecutar_todo`, conectarlo a *serverless* y pulsar **Run all**.
4. Importar `04_visualizacion/tablero_siniestralidad.lvdash.json` en **Dashboards** para ver el tablero nativo.
5. Crear un *Job* sobre `ejecutar_todo` para registrar las ejecuciones en `monitoreo_ejecuciones`.

El CSV completo no se incluye por su tamaño. `99_evidencias/muestra_anonimizada.csv` trae una muestra que solo contiene combinaciones con 5 casos o más.

## Uso de inteligencia artificial
Se usó Claude (Anthropic). El detalle está en el anexo del documento APA.

## Conexión externa
`04_visualizacion/conexion_externa.py` lee `vw_priorizacion` desde fuera de Databricks, a través de un SQL Warehouse. Se corre en un computador local, con la clave en una variable de entorno. El resultado de la prueba está en `99_evidencias/conexion_externa.txt`.

También se probó con Power BI Desktop: se conecta con el conector Databricks al mismo SQL Warehouse y carga `vw_priorizacion`. Archivo y capturas en el paquete de entrega.
