"""Helpers para enriquecer la UI de Dagster con metadatos legibles."""
from __future__ import annotations

import dagster as dg
import pandas as pd


def _fmt(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[c]):
            out[c] = out[c].dt.strftime("%Y-%m-%d")
    return out.fillna("")


def preview(df: pd.DataFrame, n: int = 10) -> dg.MetadataValue:
    return dg.MetadataValue.md(_fmt(df.head(n)).to_markdown(index=False))


def esquema(df: pd.DataFrame) -> dg.TableSchema:
    return dg.TableSchema(
        columns=[dg.TableColumn(name=str(c), type=str(t)) for c, t in df.dtypes.items()]
    )


def resumen_por(df: pd.DataFrame, col: str, kgs: str, usd: str, top: int = 12) -> dg.MetadataValue:
    """Tabla markdown con filas, kgs y USD agrupados por `col`."""
    if df.empty or col not in df.columns:
        return dg.MetadataValue.md("_Sin datos_")
    g = (
        df.groupby(col, dropna=False)
        .agg(filas=(col, "size"), kgs=(kgs, "sum"), usd=(usd, "sum"))
        .sort_values("usd", key=abs, ascending=False)
        .head(top)
        .reset_index()
    )
    g["kgs"] = g["kgs"].map(lambda v: f"{v:,.2f}")
    g["usd"] = g["usd"].map(lambda v: f"$ {v:,.2f}")
    return dg.MetadataValue.md(g.to_markdown(index=False))


def base(df: pd.DataFrame, kgs: str | None = None, usd: str | None = None, op: str | None = None) -> dict:
    """Metadatos estándar: conteo de filas, esquema, vista previa y totales."""
    md: dict = {
        "dagster/row_count": dg.MetadataValue.int(len(df)),
        "dagster/column_schema": esquema(df),
        "vista_previa": preview(df),
    }
    if kgs and kgs in df.columns:
        md["total_kgs"] = dg.MetadataValue.float(float(pd.to_numeric(df[kgs], errors="coerce").sum()))
    if usd and usd in df.columns:
        md["total_usd"] = dg.MetadataValue.float(float(pd.to_numeric(df[usd], errors="coerce").sum()))
    if op and op in df.columns:
        md["ops_distintas"] = dg.MetadataValue.int(int(df[op].nunique()))
    return md
