"""
### TraceOP · Resumen de costos por OP

Se ejecuta **automáticamente** cuando `traceop_etl_anual` actualiza las tablas de
movimientos (programación por *Assets*). También puede lanzarse manualmente para
recalcular el resumen.

1. **Calcular resumen:** agrupa por OP × categoría (kg netos, valorizado y precio unitario
   ponderado), reemplaza `resumen_op.db` y concilia los USD contra las transacciones de origen.
2. **Publicar datos para el dashboard:** top 10 de OPs y marca de última actualización.
"""

import pendulum
from airflow.sdk import dag, task

from comun.activos import CONSUMO_HILADO, DATOS_DASHBOARD, MOV_HILADO, MOV_TEJEDURIA, RESUMEN_OP


@dag(
    dag_id="traceop_resumen_op",
    dag_display_name="TraceOP · Resumen de costos por OP",
    description="Consolida hilado + tejeduría en resumen_op.db",
    doc_md=__doc__,
    schedule=[MOV_HILADO, CONSUMO_HILADO, MOV_TEJEDURIA],
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Lima"),
    catchup=False,
    max_active_runs=1,
    tags=["traceop", "resumen", "dashboard"],
    default_args={"owner": "traceop", "retries": 1},
)
def traceop_resumen_op():

    @task(task_display_name="Calcular resumen por OP", outlets=[RESUMEN_OP])
    def calcular_resumen():
        from orquestador import tareas
        return tareas.calcular_resumen()

    @task(task_display_name="Publicar datos para el dashboard", outlets=[DATOS_DASHBOARD])
    def publicar_datos_dashboard():
        from orquestador import tareas
        return tareas.publicar_datos_dashboard()

    calcular_resumen() >> publicar_datos_dashboard()


traceop_resumen_op()
