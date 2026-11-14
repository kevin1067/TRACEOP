"""Etapa 1 · Extracción: lectura de los reportes Excel de trazabilidad de partidas."""

import os

import dagster as dg
import pandas as pd

from config.config import (
    COLS_ALM_TEJEDURIA,
    COLS_CONSUMO_HILADO,
    COLS_MOV_HILADO,
    COLS_MOV_TEJEDURIA,
)
from ETL.EXTRACT.extract_hilado import getExcelMovData
from orquestador import metadata as meta
from orquestador.recursos import RutasTraceOp, particiones_anio

COLUMNAS_REQUERIDAS = sorted(
    set(COLS_MOV_HILADO) | set(COLS_CONSUMO_HILADO) | set(COLS_MOV_TEJEDURIA) | set(COLS_ALM_TEJEDURIA) | {"Medida"}
)


@dg.asset(
    group_name="extraccion",
    partitions_def=particiones_anio,
    kinds={"excel", "pandas"},
    owners=["team:traceop"],
    tags={"etapa": "extract"},
    code_version="1",
    check_specs=[
        dg.AssetCheckSpec(
            "columnas_requeridas",
            asset="excel_trazabilidad_partidas",
            description="El Excel contiene todas las columnas que consumen las transformaciones.",
            blocking=True,
        ),
    ],
)
def excel_trazabilidad_partidas(context: dg.AssetExecutionContext, rutas: RutasTraceOp):
    """Consolida los reportes mensuales **rptTrazabilidadPartidas** del año en un solo DataFrame.

    Fuente: `datos_excel/<año>/*.xlsx` · Alimenta a hilado y tejeduría.
    """
    anio = context.partition_key
    carpeta = rutas.carpeta_anio(anio)
    context.log.info(f"Leyendo reportes Excel desde {carpeta}")

    df = getExcelMovData(carpeta)

    archivos = sorted(
        f for f in os.listdir(carpeta) if f.lower().endswith((".xlsx", ".xlsm"))
    )
    faltantes = [c for c in COLUMNAS_REQUERIDAS if c not in df.columns]

    yield dg.Output(
        df,
        metadata={
            **meta.base(df, kgs="Kgs", usd="dolares", op="OP Referencial"),
            "archivos_leidos": dg.MetadataValue.int(len(archivos)),
            "archivos": dg.MetadataValue.md("\n".join(f"- `{a}`" for a in archivos)),
            "carpeta_origen": dg.MetadataValue.path(str(carpeta)),
            "por_aspecto": meta.resumen_por(df, "Aspecto", "Kgs", "dolares"),
        },
    )
    yield dg.AssetCheckResult(
        check_name="columnas_requeridas",
        passed=not faltantes,
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={
            "columnas_faltantes": dg.MetadataValue.json(faltantes),
            "columnas_esperadas": dg.MetadataValue.int(len(COLUMNAS_REQUERIDAS)),
        },
    )
