"""Etapa 3 · Carga: migración a las bases SQLite de control.

Cada carga es **idempotente por año**: antes de insertar se eliminan las filas del
mismo `anio_registro`, de modo que re-ejecutar una partición no duplica datos.
"""

import dagster as dg
import pandas as pd

from ETL.LOAD.load_hilado import MigrarHilado
from ETL.LOAD.load_tejido import MigrarTejido
from orquestador.recursos import RutasTraceOp, particiones_anio

COMUNES = dict(
    group_name="carga_sqlite",
    partitions_def=particiones_anio,
    kinds={"sqlite"},
    owners=["team:traceop"],
    tags={"etapa": "load"},
    code_version="1",
)


def _conciliacion(asset: str) -> dg.AssetCheckSpec:
    return dg.AssetCheckSpec(
        "conciliacion_filas",
        asset=asset,
        description="Las filas en SQLite para el año coinciden con las filas transformadas.",
    )


def _resultado(migrador, tabla: str, anio: int, esperadas: int, borradas: int, db) -> dg.MaterializeResult:
    cur = migrador.conn.execute(
        f"SELECT COUNT(*), COALESCE(SUM(kgs),0), COALESCE(SUM(dolares),0) FROM {tabla} WHERE anio_registro = ?",
        (anio,),
    )
    en_db, kgs, usd = cur.fetchone()
    total = migrador.conn.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]
    migrador.conn.close()
    return dg.MaterializeResult(
        metadata={
            "dagster/row_count": dg.MetadataValue.int(int(en_db)),
            "tabla": dg.MetadataValue.text(tabla),
            "base_de_datos": dg.MetadataValue.path(str(db)),
            "filas_reemplazadas": dg.MetadataValue.int(int(borradas)),
            "filas_totales_tabla": dg.MetadataValue.int(int(total)),
            "total_kgs": dg.MetadataValue.float(float(kgs)),
            "total_usd": dg.MetadataValue.float(float(usd)),
        },
        check_results=[
            dg.AssetCheckResult(
                check_name="conciliacion_filas",
                passed=int(en_db) == esperadas,
                severity=dg.AssetCheckSeverity.ERROR,
                metadata={
                    "filas_transformadas": dg.MetadataValue.int(esperadas),
                    "filas_en_sqlite": dg.MetadataValue.int(int(en_db)),
                },
            )
        ],
    )


def _limpiar_anio(migrador, tabla: str, anio: int) -> int:
    cur = migrador.conn.execute(f"DELETE FROM {tabla} WHERE anio_registro = ?", (anio,))
    migrador.conn.commit()
    return cur.rowcount


# ----------------------------------------------------------------------
# control_hilados.db
# ----------------------------------------------------------------------
@dg.asset(**COMUNES, check_specs=[_conciliacion("sqlite_mov_hilado")])
def sqlite_mov_hilado(context: dg.AssetExecutionContext, rutas: RutasTraceOp, movimientos_hilado: pd.DataFrame):
    """Tabla `mov_hilado` en **control_hilados.db** (+ alta automática en `alm_hilado`)."""
    anio = int(context.partition_key)
    m = MigrarHilado(str(rutas.db_hilado))
    borradas = _limpiar_anio(m, "mov_hilado", anio)
    m.migrar_movimientos(movimientos_hilado)
    esperadas = int((~movimientos_hilado["Cod Hilado"].astype(str).str.strip().isin(m.CODIGOS_INVALIDOS)).sum())
    return _resultado(m, "mov_hilado", anio, esperadas, borradas, rutas.db_hilado)


@dg.asset(**COMUNES, check_specs=[_conciliacion("sqlite_consumo_hilado")], deps=["sqlite_mov_hilado"])
def sqlite_consumo_hilado(context: dg.AssetExecutionContext, rutas: RutasTraceOp, consumo_hilado: pd.DataFrame):
    """Tabla `consumo_hilado` en **control_hilados.db**."""
    anio = int(context.partition_key)
    m = MigrarHilado(str(rutas.db_hilado))
    borradas = _limpiar_anio(m, "consumo_hilado", anio)
    m.migrar_consumos(consumo_hilado)
    esperadas = int((~consumo_hilado["Cod Hilado"].astype(str).str.strip().isin(m.CODIGOS_INVALIDOS)).sum())
    return _resultado(m, "consumo_hilado", anio, esperadas, borradas, rutas.db_hilado)


# ----------------------------------------------------------------------
# control_tejidos.db
# ----------------------------------------------------------------------
@dg.asset(**COMUNES)
def sqlite_alm_tejeduria(rutas: RutasTraceOp, catalogo_tejeduria: pd.DataFrame):
    """Tabla maestra `alm_tejeduria` en **control_tejidos.db** (solo inserta partidas nuevas)."""
    m = MigrarTejido(str(rutas.db_tejido))
    antes = m.conn.execute("SELECT COUNT(*) FROM alm_tejeduria").fetchone()[0]
    m.migrar_catologo_tejeduria(catalogo_tejeduria)
    despues = m.conn.execute("SELECT COUNT(*) FROM alm_tejeduria").fetchone()[0]
    m.conn.close()
    return dg.MaterializeResult(metadata={
        "dagster/row_count": dg.MetadataValue.int(int(despues)),
        "partidas_nuevas": dg.MetadataValue.int(int(despues - antes)),
        "base_de_datos": dg.MetadataValue.path(str(rutas.db_tejido)),
    })


@dg.asset(**COMUNES, check_specs=[_conciliacion("sqlite_mov_tejeduria")], deps=["sqlite_alm_tejeduria"])
def sqlite_mov_tejeduria(context: dg.AssetExecutionContext, rutas: RutasTraceOp, movimientos_tejeduria: pd.DataFrame):
    """Tabla `mov_tejeduria` en **control_tejidos.db** (FK a `alm_tejeduria`)."""
    anio = int(context.partition_key)
    m = MigrarTejido(str(rutas.db_tejido))
    borradas = _limpiar_anio(m, "mov_tejeduria", anio)
    m.migrar_movimientos_tejeduria(movimientos_tejeduria.copy())
    return _resultado(m, "mov_tejeduria", anio, len(movimientos_tejeduria), borradas, rutas.db_tejido)
