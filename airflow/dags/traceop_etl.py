"""
### TraceOP · ETL anual de trazabilidad de costos por OP

Procesa los reportes **rptTrazabilidadPartidas** de un año y los carga en las bases
SQLite de control.

| Etapa | Qué hace | Código reutilizado |
|---|---|---|
| **Extracción** | Valida y consolida los Excel de `datos_excel/<año>` | `getExcelMovData` |
| **Transformación** | Reglas de negocio de hilado y tejeduría | `TransformHilado`, `TransformTejido` |
| **Calidad** | OP informada, tipo E/S, categoría mapeada, duplicados | — |
| **Carga** | Reemplaza el año en SQLite y concilia filas | `MigrarHilado`, `MigrarTejido` |

Al terminar publica los **Assets** de las tablas cargadas, lo que dispara
automáticamente el DAG `traceop_resumen_op`.

**Ejecución manual:** *Trigger* → elegir el **año** en el formulario.
"""

from datetime import datetime, timedelta

import pendulum
from airflow.sdk import Param, dag, get_current_context, task, task_group

from comun.activos import ALM_TEJEDURIA, CONSUMO_HILADO, MOV_HILADO, MOV_TEJEDURIA

ANIO_ACTUAL = datetime.now().year
ANIOS = [str(a) for a in range(ANIO_ACTUAL - 4, ANIO_ACTUAL + 1)]


def _tareas():
    # Import diferido: el DAG se parsea rápido sin cargar pandas
    from orquestador import tareas
    return tareas


def _anio() -> str:
    return str(get_current_context()["params"]["anio"])


@dag(
    dag_id="traceop_etl_anual",
    dag_display_name="TraceOP · ETL anual de trazabilidad",
    description="Excel → transformación → calidad → SQLite, por año",
    doc_md=__doc__,
    schedule="0 6 1 * *",  # cierre mensual: día 1, 06:00
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Lima"),
    catchup=False,
    max_active_runs=1,
    tags=["traceop", "etl", "hilado", "tejeduria"],
    params={
        "anio": Param(
            str(ANIO_ACTUAL),
            type="string",
            enum=ANIOS,
            title="Año a procesar",
            description="Carpeta datos_excel/<año> que se va a procesar.",
        ),
    },
    default_args={"owner": "traceop", "retries": 1, "retry_delay": timedelta(seconds=30)},
)
def traceop_etl_anual():

    @task_group(group_id="extraccion", tooltip="Lectura de los reportes Excel")
    def extraccion():
        @task(task_display_name="Validar fuentes Excel")
        def validar_fuentes():
            return _tareas().validar_fuentes(_anio())

        @task(task_display_name="Extraer y consolidar Excel")
        def extraer_excel():
            return _tareas().extraer_excel(_anio())

        @task(task_display_name="Validar columnas requeridas")
        def validar_columnas():
            return _tareas().validar_columnas(_anio())

        validar_fuentes() >> extraer_excel() >> validar_columnas()

    @task_group(group_id="transformacion", tooltip="Reglas de negocio")
    def transformacion():
        @task_group(group_id="hilado")
        def hilado():
            @task(task_display_name="Movimientos de hilado")
            def mov_hilado():
                return _tareas().transformar_mov_hilado(_anio())

            @task(task_display_name="Consumo y merma de hilado")
            def consumo_hilado():
                return _tareas().transformar_consumo_hilado(_anio())

            mov_hilado()
            consumo_hilado()

        @task_group(group_id="tejeduria")
        def tejeduria():
            @task(task_display_name="Catálogo de partidas")
            def catalogo_tejeduria():
                return _tareas().transformar_catalogo_tejeduria(_anio())

            @task(task_display_name="Movimientos de tejeduría")
            def mov_tejeduria():
                return _tareas().transformar_mov_tejeduria(_anio())

            catalogo_tejeduria()
            mov_tejeduria()

        hilado()
        tejeduria()

    @task_group(group_id="calidad", tooltip="Reglas de calidad de datos (bloqueantes y de advertencia)")
    def calidad():
        @task(task_display_name="DQ · movimientos de hilado")
        def dq_mov_hilado():
            return _tareas().validar_mov_hilado(_anio())

        @task(task_display_name="DQ · consumo de hilado")
        def dq_consumo_hilado():
            return _tareas().validar_consumo_hilado(_anio())

        @task(task_display_name="DQ · catálogo de partidas")
        def dq_catalogo_tejeduria():
            return _tareas().validar_catalogo_tejeduria(_anio())

        @task(task_display_name="DQ · movimientos de tejeduría")
        def dq_mov_tejeduria():
            return _tareas().validar_mov_tejeduria(_anio())

        dq_mov_hilado()
        dq_consumo_hilado()
        dq_catalogo_tejeduria()
        dq_mov_tejeduria()

    @task_group(group_id="carga", tooltip="Migración idempotente a SQLite")
    def carga():
        # Cada base SQLite admite un solo escritor: las cargas de una misma base van en serie
        @task(task_display_name="SQLite · mov_hilado", outlets=[MOV_HILADO])
        def cargar_mov_hilado():
            return _tareas().cargar_mov_hilado(_anio())

        @task(task_display_name="SQLite · consumo_hilado", outlets=[CONSUMO_HILADO])
        def cargar_consumo_hilado():
            return _tareas().cargar_consumo_hilado(_anio())

        @task(task_display_name="SQLite · alm_tejeduria", outlets=[ALM_TEJEDURIA])
        def cargar_catalogo_tejeduria():
            return _tareas().cargar_catalogo_tejeduria(_anio())

        @task(task_display_name="SQLite · mov_tejeduria", outlets=[MOV_TEJEDURIA])
        def cargar_mov_tejeduria():
            return _tareas().cargar_mov_tejeduria(_anio())

        cargar_mov_hilado() >> cargar_consumo_hilado()
        cargar_catalogo_tejeduria() >> cargar_mov_tejeduria()

    @task(task_display_name="Limpiar staging")
    def limpiar_staging():
        return _tareas().limpiar_staging(_anio())

    extraccion() >> transformacion() >> calidad() >> carga() >> limpiar_staging()


traceop_etl_anual()
