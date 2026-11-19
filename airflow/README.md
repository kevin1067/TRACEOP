> **Implementación futura.** Este documento describe la orquestación del ETL con Apache Airflow.
> El prototipo actual se ejecuta con `main.py`; ver el [README principal](../README.md).

# Orquestador del ETL (Apache Airflow)

El ETL se orquesta con **Apache Airflow 3.2.2**, que corre en Docker. Los DAGs llaman al mismo código de `ETL/` y `config/`, sin modificarlo. La lógica de cada tarea está en [`orquestador/tareas.py`](../orquestador/tareas.py), que no depende de Airflow.

## Requisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) con al menos 4 GB de RAM asignados. Airflow no corre de forma nativa en Windows.
- Los Excel en `datos_excel/<año>/` y la carpeta `migracion/` (las mismas que usa `main.py`).

## Ejecutar

Desde la raíz del proyecto:

```powershell
.\orquestador.ps1            # construye la imagen, levanta Airflow y abre http://localhost:8080
.\orquestador.ps1 logs       # logs del scheduler
.\orquestador.ps1 detener    # detiene los contenedores
```

Usuario **airflow**, clave **airflow**. Los DAGs empiezan pausados: actívalos con el interruptor de cada uno.

## DAGs

### `traceop_etl_anual`: ETL anual de trazabilidad

```
extraccion                 transformacion                calidad                     carga
┌──────────────────┐      ┌─ hilado ─────────────┐      ┌───────────────────┐      ┌───────────────────────────────┐
│ validar_fuentes  │      │ mov_hilado           │      │ dq_mov_hilado     │      │ mov_hilado → consumo_hilado   │ ──► Asset
│ extraer_excel    │ ───► │ consumo_hilado       │ ───► │ dq_consumo_hilado │ ───► │                               │
│ validar_columnas │      ├─ tejeduria ──────────┤      │ dq_catalogo       │      │ alm_tejeduria → mov_tejeduria │ ──► Asset
└──────────────────┘      │ catalogo_tejeduria   │      │ dq_mov_tejeduria  │      └───────────────────────────────┘
                          │ mov_tejeduria        │      └───────────────────┘                  │
                          └──────────────────────┘                                    limpiar_staging
```

- **Año como parámetro:** al pulsar *Trigger* aparece un formulario para elegir el año. Reemplaza la pregunta por consola `prompt_year()`.
- **Programación:** el día 1 de cada mes a las 06:00 (hora de Lima) procesa el año en curso.
- **Calidad de datos:** OP informada y tipo de movimiento ∈ {E, S} son reglas bloqueantes (si fallan, la tarea falla y no se carga nada). Categoría mapeada en `COD_DICT` y duplicados son advertencias en el log.
- **Cargas idempotentes:** antes de insertar se borran las filas del mismo `anio_registro` y luego se concilian las filas transformadas contra las filas en SQLite. Volver a ejecutar un año no duplica datos.
- **Métricas en XCom:** cada tarea devuelve filas, kg, USD, OPs y un desglose por categoría (pestaña *XCom* de la tarea).

### `traceop_resumen_op`: Resumen de costos por OP

Se dispara **automáticamente** cuando el DAG anual actualiza los Assets `traceop_mov_hilado`, `traceop_consumo_hilado` y `traceop_mov_tejeduria` (programación por datos). Calcula el resumen OP × categoría, reemplaza `resumen_op.db`, concilia los USD contra el origen y publica el top 10 de OPs para el dashboard.

| Tu código | En Airflow |
|---|---|
| `PipelineHilado` | `traceop_etl_anual` → grupos `transformacion.hilado` y `carga` |
| `PipelineTejeduria` | `traceop_etl_anual` → grupos `transformacion.tejeduria` y `carga` |
| `PipelineResumen` | `traceop_resumen_op` |
| `prompt_year()` | Parámetro `anio` del formulario *Trigger* |
| `setup_environment_variables()` | Variables `TRACEOP_*` en `airflow/docker-compose.yaml` |

## Estructura

```
airflow/
├── docker-compose.yaml        # Postgres + api-server + scheduler + dag-processor (LocalExecutor)
├── Dockerfile                 # imagen oficial apache/airflow:3.2.2 + pandas/openpyxl
├── requirements-airflow.txt
└── dags/
    ├── traceop_etl.py         # DAG anual
    ├── traceop_resumen.py     # DAG de resumen (por Assets)
    └── comun/activos.py       # Assets compartidos
orquestador/tareas.py          # lógica de cada tarea (reutiliza ETL/)
```

El proyecto completo se monta en el contenedor en `/opt/airflow/traceop`. Entre tareas, los DataFrames se pasan por `staging/<año>/`, porque XCom no está pensado para cientos de miles de filas, y esa carpeta se borra al final.

## Guion sugerido para la demo

1. **DAGs:** mostrar los dos DAGs con sus etiquetas y la documentación (*Docs*).
2. **Trigger** de `traceop_etl_anual` → elegir `2026` en el formulario.
3. Vista **Graph:** ver cómo avanzan los grupos Extracción → Transformación → Calidad → Carga. Abrir el **Gantt** para ver qué tareas corren en paralelo.
4. Abrir una tarea de `calidad` → **Logs** (reglas OK/FALLA) y **XCom** (métricas).
5. **Assets:** ver cómo la carga dispara sola `traceop_resumen_op`.
6. En `publicar_datos_dashboard` → **XCom**: top 10 de OPs por valorizado.
