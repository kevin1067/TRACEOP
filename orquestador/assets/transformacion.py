"""Etapa 2 · Transformación: reglas de negocio de hilado y tejeduría."""

import dagster as dg
import pandas as pd

from config.config import (
    ASPECTO_CONSUMO_HILADO,
    ASPECTO_MOV_HILADO,
    ASPECTO_MOV_TEJEDURIA,
    COD_DICT,
    COLS_ALM_TEJEDURIA,
    COLS_CONSUMO_HILADO,
    COLS_MOV_HILADO,
    COLS_MOV_TEJEDURIA,
)
from ETL.TRANSFORM.transform_alm_hilado import TransformHilado
from ETL.TRANSFORM.transform_alm_tejeduria import TransformTejido
from orquestador import metadata as meta
from orquestador.recursos import particiones_anio

COMUNES = dict(
    group_name="transformacion",
    partitions_def=particiones_anio,
    kinds={"pandas", "python"},
    owners=["team:traceop"],
    tags={"etapa": "transform"},
    code_version="1",
)


# ----------------------------------------------------------------------
# Reglas de calidad reutilizables
# ----------------------------------------------------------------------
def _spec(asset: str, nombre: str, descripcion: str) -> dg.AssetCheckSpec:
    return dg.AssetCheckSpec(nombre, asset=asset, description=descripcion)


def _check(nombre: str, malas: pd.DataFrame, total: int, severidad=dg.AssetCheckSeverity.WARN) -> dg.AssetCheckResult:
    return dg.AssetCheckResult(
        check_name=nombre,
        passed=malas.empty,
        severity=severidad,
        metadata={
            "filas_con_problema": dg.MetadataValue.int(len(malas)),
            "filas_evaluadas": dg.MetadataValue.int(total),
            "muestra": meta.preview(malas, 5) if not malas.empty else dg.MetadataValue.md("_Sin observaciones_ ✅"),
        },
    )


def _checks_movimientos(df: pd.DataFrame, op: str, tipo: str, claves_dup: list[str] | None) -> list[dg.AssetCheckResult]:
    n = len(df)
    resultados = [
        _check("op_informada", df[df[op].fillna("").astype(str).str.strip() == ""], n, dg.AssetCheckSeverity.ERROR),
        _check("tipo_mov_valido", df[~df[tipo].isin(["E", "S"])], n, dg.AssetCheckSeverity.ERROR),
        _check("categoria_asignada", df[df["categoria"].isna()], n),
    ]
    if claves_dup:
        resultados.append(_check("sin_duplicados", df[df.duplicated(subset=claves_dup, keep=False)], n))
    return resultados


def SPECS_MOV(asset: str, dup: bool = True) -> list[dg.AssetCheckSpec]:
    specs = [
        _spec(asset, "op_informada", "Toda fila tiene OP Referencial."),
        _spec(asset, "tipo_mov_valido", "Tipo de movimiento ∈ {E, S} (restricción CHECK de la tabla)."),
        _spec(asset, "categoria_asignada", "Cada código de transacción está mapeado en COD_DICT."),
    ]
    if dup:
        specs.append(_spec(asset, "sin_duplicados", "Sin duplicados por OP + Num.Mov. + Kgs."))
    return specs


# ----------------------------------------------------------------------
# Hilado
# ----------------------------------------------------------------------
@dg.asset(**COMUNES, check_specs=SPECS_MOV("movimientos_hilado"))
def movimientos_hilado(context: dg.AssetExecutionContext, excel_trazabilidad_partidas: pd.DataFrame):
    """Movimientos de hilado (envíos, devoluciones, sustento de costo).

    Reglas: normalización de OP, propagación de OC en **Y18**, negativización de **X18**,
    deduplicación y recálculo del costo unitario de teñido (**X34/L34**).
    """
    t = TransformHilado(excel_trazabilidad_partidas, COD_DICT)
    df = t.getMovHilado(int(context.partition_key), ASPECTO_MOV_HILADO, COLS_MOV_HILADO)

    yield dg.Output(df, metadata={
        **meta.base(df, kgs="Kgs", usd="dolares", op="OP Referencial"),
        "por_categoria": meta.resumen_por(df, "categoria", "Kgs", "dolares"),
    })
    yield from _checks_movimientos(df, "OP Referencial", "Tipo", ["OP Referencial", "Num.Mov.", "Kgs"])


@dg.asset(**COMUNES, check_specs=SPECS_MOV("consumo_hilado", dup=False))
def consumo_hilado(context: dg.AssetExecutionContext, excel_trazabilidad_partidas: pd.DataFrame):
    """Hilado consumido y merma en tejeduría (transacciones **JE4C / JE4M**)."""
    t = TransformHilado(excel_trazabilidad_partidas, COD_DICT)
    df = t.getMovConsumoHilado(int(context.partition_key), ASPECTO_CONSUMO_HILADO, COLS_CONSUMO_HILADO)

    yield dg.Output(df, metadata={
        **meta.base(df, kgs="Kgs", usd="dolares", op="OP Referencial"),
        "por_aspecto": meta.resumen_por(df, "Aspecto", "Kgs", "dolares"),
    })
    yield from _checks_movimientos(df, "OP Referencial", "Tipo", None)


# ----------------------------------------------------------------------
# Tejeduría
# ----------------------------------------------------------------------
@dg.asset(
    **COMUNES,
    check_specs=[_spec("catalogo_tejeduria", "partida_unica", "Cada partida aparece una sola vez (PK).")],
)
def catalogo_tejeduria(excel_trazabilidad_partidas: pd.DataFrame):
    """Catálogo maestro de partidas de tela (tela, combinación y color)."""
    df = TransformTejido(excel_trazabilidad_partidas, COD_DICT).getAlmTejeduria(COLS_ALM_TEJEDURIA)

    yield dg.Output(df, metadata={
        **meta.base(df),
        "partidas": dg.MetadataValue.int(int(df["Partida"].nunique())),
        "telas_distintas": dg.MetadataValue.int(int(df["Cod_Tela"].nunique())),
    })
    yield _check("partida_unica", df[df.duplicated(subset=["Partida"], keep=False)], len(df), dg.AssetCheckSeverity.ERROR)


@dg.asset(**COMUNES, check_specs=SPECS_MOV("movimientos_tejeduria", dup=False))
def movimientos_tejeduria(context: dg.AssetExecutionContext, excel_trazabilidad_partidas: pd.DataFrame):
    """Movimientos de tela: tejeduría, tintorería, post-teñido y confección.

    Reglas: deduplicación de **TCM**, categoría *Tela principal* y costo unitario
    de tintorería **T21** sobre los kilos despachados en **JS8**.
    """
    t = TransformTejido(excel_trazabilidad_partidas, COD_DICT)
    df = t.getMovTejeduria(int(context.partition_key), ASPECTO_MOV_TEJEDURIA, COLS_MOV_TEJEDURIA)

    yield dg.Output(df, metadata={
        **meta.base(df, kgs="Kgs", usd="dolares", op="OP Referencial"),
        "por_categoria": meta.resumen_por(df, "categoria", "Kgs", "dolares"),
    })
    yield from _checks_movimientos(df, "OP Referencial", "Tipo", None)
