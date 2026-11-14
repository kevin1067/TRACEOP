"""Etapa 4 · Analítica: resumen de costos por OP y consumo en Power BI."""

import dagster as dg
import numpy as np
import pandas as pd

from config.config import COD_DICT
from ETL.EXTRACT.extract_hilado import getTransacciones
from ETL.LOAD.load_hilado import MigrarHilado
from ETL.LOAD.load_resumen import MigrarResumen
from ETL.LOAD.load_tejido import MigrarTejido
from ETL.TRANSFORM.transform_resumen import TransformResumen
from orquestador import metadata as meta
from orquestador.recursos import RutasTraceOp


@dg.asset(
    group_name="analitica",
    deps=["sqlite_mov_hilado", "sqlite_consumo_hilado", "sqlite_mov_tejeduria"],
    kinds={"sqlite", "pandas"},
    owners=["team:traceop"],
    tags={"etapa": "load"},
    code_version="1",
    check_specs=[
        dg.AssetCheckSpec("conciliacion_usd", asset="resumen_op",
                          description="El valorizado del resumen cuadra con la suma de transacciones origen."),
        dg.AssetCheckSpec("precio_unitario_valido", asset="resumen_op",
                          description="Precio unitario ponderado finito en todas las filas."),
    ],
)
def resumen_op(rutas: RutasTraceOp):
    """Resumen consolidado por **OP × categoría**: kg netos, valorizado y precio unitario ponderado.

    Integra todos los años cargados en `control_hilados.db` y `control_tejidos.db`
    y reemplaza por completo la tabla `resumen_op` en **resumen_op.db**.
    """
    # Garantiza que las tablas existan aunque una etapa aún no se haya cargado
    MigrarHilado(str(rutas.db_hilado)).conn.close()
    MigrarTejido(str(rutas.db_tejido)).conn.close()

    origen = getTransacciones(rutas.db_hilado, rutas.db_tejido)
    resumen = TransformResumen(origen, COD_DICT).getResumen()

    m = MigrarResumen(str(rutas.db_resumen))
    m.conn.execute("DELETE FROM resumen_op")
    m.conn.commit()
    m.migrar_resumen(resumen)
    m.conn.close()

    filtrado = origen[(origen["kgs"] != 0) & (origen["dolares"] != 0)]
    usd_origen = round(float(filtrado["dolares"].sum()), 2)
    usd_resumen = round(float(resumen["valorizado_total"].sum()), 2)

    top_ops = (
        resumen.groupby("op")[["kg_netos", "valorizado_total"]].sum()
        .sort_values("valorizado_total", ascending=False).head(10).reset_index()
    )
    pu = pd.to_numeric(resumen["precio_unitario_ponderado"], errors="coerce").astype("float64")
    pu_invalidos = resumen[~np.isfinite(pu)]

    return dg.MaterializeResult(
        metadata={
            **meta.base(resumen, kgs="kg_netos", usd="valorizado_total", op="op"),
            "transacciones_origen": dg.MetadataValue.int(len(origen)),
            "top_10_ops_por_valorizado": dg.MetadataValue.md(top_ops.to_markdown(index=False, floatfmt=",.2f")),
            "por_categoria": meta.resumen_por(resumen, "categoria", "kg_netos", "valorizado_total", top=20),
            "base_de_datos": dg.MetadataValue.path(str(rutas.db_resumen)),
        },
        check_results=[
            dg.AssetCheckResult(
                check_name="conciliacion_usd",
                passed=abs(usd_origen - usd_resumen) <= max(1.0, abs(usd_origen) * 1e-6),
                metadata={
                    "usd_origen": dg.MetadataValue.float(usd_origen),
                    "usd_resumen": dg.MetadataValue.float(usd_resumen),
                    "diferencia": dg.MetadataValue.float(round(usd_origen - usd_resumen, 2)),
                },
            ),
            dg.AssetCheckResult(
                check_name="precio_unitario_valido",
                passed=pu_invalidos.empty,
                metadata={"filas_invalidas": dg.MetadataValue.int(len(pu_invalidos))},
            ),
        ],
    )


dashboard_powerbi = dg.AssetSpec(
    "dashboard_powerbi",
    deps=["resumen_op", "sqlite_mov_hilado", "sqlite_mov_tejeduria"],
    group_name="analitica",
    kinds={"powerbi"},
    owners=["team:traceop"],
    description="Tablero de trazabilidad de costos por OP en Power BI (consume las bases SQLite vía Python).",
)
