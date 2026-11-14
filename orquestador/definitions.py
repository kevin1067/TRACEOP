"""Punto de entrada de Dagster para TraceOP.

    dagster dev -m orquestador.definitions
"""

from datetime import datetime

import dagster as dg

from orquestador.assets import analitica, carga, extraccion, transformacion
from orquestador.recursos import RutasTraceOp, particiones_anio

todos_los_assets = dg.load_assets_from_modules([extraccion, transformacion, carga, analitica])
todos_los_checks = dg.load_asset_checks_from_modules([extraccion, transformacion, carga, analitica])

# ----------------------------------------------------------------------
# Jobs
# ----------------------------------------------------------------------
etl_traceop = dg.define_asset_job(
    "etl_traceop",
    selection=dg.AssetSelection.groups("extraccion", "transformacion", "carga_sqlite", "analitica"),
    partitions_def=particiones_anio,
    description="ETL completo de un año: Excel → transformación → SQLite → resumen por OP.",
    tags={"proyecto": "traceop"},
)

etl_hilado = dg.define_asset_job(
    "etl_hilado",
    selection=dg.AssetSelection.assets("excel_trazabilidad_partidas", "movimientos_hilado",
                                       "consumo_hilado", "sqlite_mov_hilado", "sqlite_consumo_hilado"),
    partitions_def=particiones_anio,
    description="Solo la cadena de hilado (equivale a PipelineHilado).",
)

etl_tejeduria = dg.define_asset_job(
    "etl_tejeduria",
    selection=dg.AssetSelection.assets("excel_trazabilidad_partidas", "catalogo_tejeduria",
                                       "movimientos_tejeduria", "sqlite_alm_tejeduria", "sqlite_mov_tejeduria"),
    partitions_def=particiones_anio,
    description="Solo la cadena de tejeduría (equivale a PipelineTejeduria).",
)

recalcular_resumen = dg.define_asset_job(
    "recalcular_resumen",
    selection=dg.AssetSelection.assets("resumen_op"),
    description="Recalcula resumen_op.db a partir de las bases ya cargadas (equivale a PipelineResumen).",
)


# ----------------------------------------------------------------------
# Automatización
# ----------------------------------------------------------------------
@dg.schedule(
    job=etl_traceop,
    cron_schedule="0 6 1 * *",
    execution_timezone="America/Lima",
    default_status=dg.DefaultScheduleStatus.STOPPED,
    description="Cierre mensual: el día 1 a las 06:00 procesa el año en curso.",
)
def cierre_mensual(context: dg.ScheduleEvaluationContext):
    anio = str(context.scheduled_execution_time.year)
    return dg.RunRequest(partition_key=anio, run_key=f"cierre-{context.scheduled_execution_time:%Y-%m}")


@dg.sensor(
    job=etl_traceop,
    minimum_interval_seconds=60,
    default_status=dg.DefaultSensorStatus.STOPPED,
    description="Detecta Excel nuevos o modificados en datos_excel/<año> y lanza el ETL de ese año.",
)
def nuevos_excel(context: dg.SensorEvaluationContext, rutas: RutasTraceOp):
    for anio in particiones_anio.get_partition_keys():
        carpeta = rutas.carpeta_anio(anio)
        if not carpeta.is_dir():
            continue
        archivos = [f for f in carpeta.iterdir() if f.suffix.lower() in (".xlsx", ".xlsm") and not f.name.startswith("~$")]
        if not archivos:
            continue
        firma = max(int(f.stat().st_mtime) for f in archivos)
        yield dg.RunRequest(
            partition_key=anio,
            run_key=f"{anio}-{len(archivos)}-{firma}",
            tags={"disparador": "nuevos_excel"},
        )
    context.update_cursor(datetime.now().isoformat())


defs = dg.Definitions(
    assets=[*todos_los_assets, analitica.dashboard_powerbi],
    asset_checks=todos_los_checks,
    jobs=[etl_traceop, etl_hilado, etl_tejeduria, recalcular_resumen],
    schedules=[cierre_mensual],
    sensors=[nuevos_excel],
    resources={
        "rutas": RutasTraceOp(
            datos_path=dg.EnvVar("TESTING_PATH"),
            db_path=dg.EnvVar("DB_TESTING_PATH"),
        ),
    },
)
