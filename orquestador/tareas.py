"""Tareas del ETL TraceOP, independientes de Airflow.

Cada función corresponde a una tarea de los DAGs en `airflow/dags/`. Reutilizan el
código existente de `ETL/` y `config/` sin modificarlo, y devuelven un diccionario
de métricas que Airflow guarda como XCom (visible en la UI, pestaña *XCom*).

Entre tareas los DataFrames se pasan por archivos en `staging/<año>/` (XCom no es
apto para DataFrames de cientos de miles de filas).

Rutas (variables de entorno):
    TRACEOP_DATOS_PATH   carpeta con los Excel por año   (fallback: TESTING_PATH)
    TRACEOP_DB_PATH      carpeta de las bases SQLite     (fallback: DB_TESTING_PATH)
    TRACEOP_STAGING_PATH carpeta de intercambio temporal (fallback: ./staging)
"""

import logging
import os
import shutil
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

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
from ETL.EXTRACT.extract_hilado import getExcelMovData, getTransacciones
from ETL.LOAD.load_hilado import MigrarHilado
from ETL.LOAD.load_resumen import MigrarResumen
from ETL.LOAD.load_tejido import MigrarTejido
from ETL.TRANSFORM.transform_alm_hilado import TransformHilado
from ETL.TRANSFORM.transform_alm_tejeduria import TransformTejido
from ETL.TRANSFORM.transform_resumen import TransformResumen

load_dotenv()
log = logging.getLogger("traceop")

COLUMNAS_REQUERIDAS = sorted(
    set(COLS_MOV_HILADO) | set(COLS_CONSUMO_HILADO) | set(COLS_MOV_TEJEDURIA)
    | set(COLS_ALM_TEJEDURIA) | {"Medida"}
)


class CalidadError(Exception):
    """Una regla de calidad bloqueante no se cumplió."""


# ----------------------------------------------------------------------
# Rutas y staging
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class Rutas:
    datos: Path
    db: Path
    staging: Path

    @property
    def db_hilado(self) -> Path:
        return self.db / "control_hilados.db"

    @property
    def db_tejido(self) -> Path:
        return self.db / "control_tejidos.db"

    @property
    def db_resumen(self) -> Path:
        return self.db / "resumen_op.db"


def rutas() -> Rutas:
    def _env(*nombres: str, defecto: str) -> Path:
        for n in nombres:
            if os.getenv(n):
                return Path(os.environ[n])
        return Path(defecto)

    r = Rutas(
        datos=_env("TRACEOP_DATOS_PATH", "TESTING_PATH", defecto="datos_excel"),
        db=_env("TRACEOP_DB_PATH", "DB_TESTING_PATH", defecto="migracion"),
        staging=_env("TRACEOP_STAGING_PATH", defecto="staging"),
    )
    r.db.mkdir(parents=True, exist_ok=True)
    return r


def _guardar(df: pd.DataFrame, anio: str, nombre: str) -> Path:
    carpeta = rutas().staging / str(anio)
    carpeta.mkdir(parents=True, exist_ok=True)
    ruta = carpeta / f"{nombre}.pkl"
    df.to_pickle(ruta)
    return ruta


def _leer(anio: str, nombre: str) -> pd.DataFrame:
    ruta = rutas().staging / str(anio) / f"{nombre}.pkl"
    if not ruta.exists():
        raise FileNotFoundError(f"No existe {ruta}. Ejecute primero la tarea que lo genera.")
    return pd.read_pickle(ruta)


def limpiar_staging(anio: str) -> dict:
    carpeta = rutas().staging / str(anio)
    shutil.rmtree(carpeta, ignore_errors=True)
    return {"staging_eliminado": str(carpeta)}


# ----------------------------------------------------------------------
# Métricas para XCom / logs
# ----------------------------------------------------------------------
def _num(df: pd.DataFrame, col: str) -> float:
    return round(float(pd.to_numeric(df[col], errors="coerce").sum()), 2) if col in df.columns else 0.0


def _metricas(df: pd.DataFrame, kgs="Kgs", usd="dolares", op="OP Referencial") -> dict:
    m = {"filas": int(len(df)), "columnas": int(df.shape[1])}
    if kgs in df.columns:
        m["total_kgs"] = _num(df, kgs)
    if usd in df.columns:
        m["total_usd"] = _num(df, usd)
    if op in df.columns:
        m["ops_distintas"] = int(df[op].nunique())
    return m


def _desglose(df: pd.DataFrame, col: str, kgs="Kgs", usd="dolares", top: int = 10) -> list[dict]:
    if df.empty or col not in df.columns:
        return []
    g = (
        df.groupby(col, dropna=False)
        .agg(filas=(col, "size"), kgs=(kgs, "sum"), usd=(usd, "sum"))
        .sort_values("usd", key=abs, ascending=False)
        .head(top)
        .reset_index()
    )
    log.info("Desglose por %s:\n%s", col, g.to_string(index=False, float_format=lambda v: f"{v:,.2f}"))
    return [
        {col: str(r[col]), "filas": int(r["filas"]), "kgs": round(float(r["kgs"]), 2), "usd": round(float(r["usd"]), 2)}
        for _, r in g.iterrows()
    ]


# ----------------------------------------------------------------------
# 1 · Extracción
# ----------------------------------------------------------------------
def validar_fuentes(anio: str) -> dict:
    carpeta = rutas().datos / str(anio)
    if not carpeta.is_dir():
        raise FileNotFoundError(f"No existe la carpeta de origen {carpeta}")
    archivos = sorted(
        f.name for f in carpeta.iterdir()
        if f.suffix.lower() in (".xlsx", ".xlsm") and not f.name.startswith("~$")
    )
    if not archivos:
        raise FileNotFoundError(f"No hay reportes Excel en {carpeta}")
    log.info("Se encontraron %d reportes en %s:\n  - %s", len(archivos), carpeta, "\n  - ".join(archivos))
    return {"carpeta": str(carpeta), "n_archivos": len(archivos), "archivos": archivos}


def extraer_excel(anio: str) -> dict:
    df = getExcelMovData(rutas().datos / str(anio))
    _guardar(df, anio, "excel")
    return {**_metricas(df), "por_aspecto": _desglose(df, "Aspecto")}


def validar_columnas(anio: str) -> dict:
    df = _leer(anio, "excel")
    faltantes = [c for c in COLUMNAS_REQUERIDAS if c not in df.columns]
    if faltantes:
        raise CalidadError(f"Faltan columnas requeridas en el Excel: {faltantes}")
    return {"columnas_requeridas": len(COLUMNAS_REQUERIDAS), "faltantes": []}


# ----------------------------------------------------------------------
# 2 · Transformación
# ----------------------------------------------------------------------
def transformar_mov_hilado(anio: str) -> dict:
    df = TransformHilado(_leer(anio, "excel"), COD_DICT).getMovHilado(int(anio), ASPECTO_MOV_HILADO, COLS_MOV_HILADO)
    _guardar(df, anio, "mov_hilado")
    return {**_metricas(df), "por_categoria": _desglose(df, "categoria")}


def transformar_consumo_hilado(anio: str) -> dict:
    df = TransformHilado(_leer(anio, "excel"), COD_DICT).getMovConsumoHilado(
        int(anio), ASPECTO_CONSUMO_HILADO, COLS_CONSUMO_HILADO)
    _guardar(df, anio, "consumo_hilado")
    return {**_metricas(df), "por_aspecto": _desglose(df, "Aspecto")}


def transformar_catalogo_tejeduria(anio: str) -> dict:
    df = TransformTejido(_leer(anio, "excel"), COD_DICT).getAlmTejeduria(COLS_ALM_TEJEDURIA)
    _guardar(df, anio, "catalogo_tejeduria")
    return {"filas": int(len(df)), "partidas": int(df["Partida"].nunique()), "telas": int(df["Cod_Tela"].nunique())}


def transformar_mov_tejeduria(anio: str) -> dict:
    df = TransformTejido(_leer(anio, "excel"), COD_DICT).getMovTejeduria(
        int(anio), ASPECTO_MOV_TEJEDURIA, COLS_MOV_TEJEDURIA)
    _guardar(df, anio, "mov_tejeduria")
    return {**_metricas(df), "por_categoria": _desglose(df, "categoria")}


# ----------------------------------------------------------------------
# 3 · Calidad
# ----------------------------------------------------------------------
def _regla(nombre: str, malas: pd.DataFrame, total: int, bloqueante: bool) -> dict:
    ok = malas.empty
    nivel = logging.INFO if ok else (logging.ERROR if bloqueante else logging.WARNING)
    log.log(nivel, "%s %-20s %d/%d filas con problema", "OK " if ok else "FALLA", nombre, len(malas), total)
    if not ok:
        log.log(nivel, "Muestra:\n%s", malas.head(5).to_string())
    return {"regla": nombre, "ok": ok, "bloqueante": bloqueante, "filas_con_problema": int(len(malas))}


def _cerrar(reglas: list[dict], anio: str, nombre: str) -> dict:
    fallas = [r["regla"] for r in reglas if not r["ok"] and r["bloqueante"]]
    if fallas:
        raise CalidadError(f"{nombre} ({anio}): fallaron reglas bloqueantes {fallas}")
    return {"reglas": reglas, "aprobadas": sum(r["ok"] for r in reglas), "total": len(reglas)}


def _reglas_movimientos(df: pd.DataFrame, duplicados: list[str] | None) -> list[dict]:
    n = len(df)
    reglas = [
        _regla("op_informada", df[df["OP Referencial"].fillna("").astype(str).str.strip() == ""], n, True),
        _regla("tipo_mov_valido", df[~df["Tipo"].isin(["E", "S"])], n, True),
        _regla("categoria_asignada", df[df["categoria"].isna()], n, False),
    ]
    if duplicados:
        reglas.append(_regla("sin_duplicados", df[df.duplicated(subset=duplicados, keep=False)], n, False))
    return reglas


def validar_mov_hilado(anio: str) -> dict:
    df = _leer(anio, "mov_hilado")
    return _cerrar(_reglas_movimientos(df, ["OP Referencial", "Num.Mov.", "Kgs"]), anio, "mov_hilado")


def validar_consumo_hilado(anio: str) -> dict:
    return _cerrar(_reglas_movimientos(_leer(anio, "consumo_hilado"), None), anio, "consumo_hilado")


def validar_mov_tejeduria(anio: str) -> dict:
    return _cerrar(_reglas_movimientos(_leer(anio, "mov_tejeduria"), None), anio, "mov_tejeduria")


def validar_catalogo_tejeduria(anio: str) -> dict:
    df = _leer(anio, "catalogo_tejeduria")
    reglas = [
        _regla("partida_unica", df[df.duplicated(subset=["Partida"], keep=False)], len(df), True),
        _regla("partida_informada", df[df["Partida"].astype(str).str.strip() == ""], len(df), True),
    ]
    return _cerrar(reglas, anio, "catalogo_tejeduria")


# ----------------------------------------------------------------------
# 4 · Carga (idempotente por año)
# ----------------------------------------------------------------------
def _borrar_anio(conn: sqlite3.Connection, tabla: str, anio: int) -> int:
    cur = conn.execute(f"DELETE FROM {tabla} WHERE anio_registro = ?", (anio,))
    conn.commit()
    return cur.rowcount


def _conciliar(conn: sqlite3.Connection, tabla: str, anio: int, esperadas: int, borradas: int) -> dict:
    en_db, kgs, usd = conn.execute(
        f"SELECT COUNT(*), COALESCE(SUM(kgs),0), COALESCE(SUM(dolares),0) FROM {tabla} WHERE anio_registro = ?",
        (anio,),
    ).fetchone()
    total = conn.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]
    conn.close()
    log.info("Conciliación %s %s: transformadas=%d · en SQLite=%d · reemplazadas=%d",
             tabla, anio, esperadas, en_db, borradas)
    if int(en_db) != esperadas:
        raise CalidadError(f"Conciliación fallida en {tabla}: {esperadas} filas transformadas vs {en_db} en SQLite")
    return {
        "tabla": tabla, "filas_cargadas": int(en_db), "filas_reemplazadas": int(borradas),
        "filas_totales_tabla": int(total), "total_kgs": round(float(kgs), 2), "total_usd": round(float(usd), 2),
    }


def _validas_hilado(df: pd.DataFrame) -> int:
    return int((~df["Cod Hilado"].astype(str).str.strip().isin(MigrarHilado.CODIGOS_INVALIDOS)).sum())


def cargar_mov_hilado(anio: str) -> dict:
    df, a = _leer(anio, "mov_hilado"), int(anio)
    m = MigrarHilado(str(rutas().db_hilado))
    borradas = _borrar_anio(m.conn, "mov_hilado", a)
    m.migrar_movimientos(df)
    return _conciliar(m.conn, "mov_hilado", a, _validas_hilado(df), borradas)


def cargar_consumo_hilado(anio: str) -> dict:
    df, a = _leer(anio, "consumo_hilado"), int(anio)
    m = MigrarHilado(str(rutas().db_hilado))
    borradas = _borrar_anio(m.conn, "consumo_hilado", a)
    m.migrar_consumos(df)
    return _conciliar(m.conn, "consumo_hilado", a, _validas_hilado(df), borradas)


def cargar_catalogo_tejeduria(anio: str) -> dict:
    df = _leer(anio, "catalogo_tejeduria")
    m = MigrarTejido(str(rutas().db_tejido))
    antes = m.conn.execute("SELECT COUNT(*) FROM alm_tejeduria").fetchone()[0]
    m.migrar_catologo_tejeduria(df)
    despues = m.conn.execute("SELECT COUNT(*) FROM alm_tejeduria").fetchone()[0]
    m.conn.close()
    return {"tabla": "alm_tejeduria", "partidas_totales": int(despues), "partidas_nuevas": int(despues - antes)}


def cargar_mov_tejeduria(anio: str) -> dict:
    df, a = _leer(anio, "mov_tejeduria"), int(anio)
    m = MigrarTejido(str(rutas().db_tejido))
    borradas = _borrar_anio(m.conn, "mov_tejeduria", a)
    m.migrar_movimientos_tejeduria(df.copy())
    return _conciliar(m.conn, "mov_tejeduria", a, len(df), borradas)


# ----------------------------------------------------------------------
# 5 · Resumen por OP
# ----------------------------------------------------------------------
def calcular_resumen() -> dict:
    r = rutas()
    # Garantiza que las tablas existan aunque una cadena aún no se haya cargado
    MigrarHilado(str(r.db_hilado)).conn.close()
    MigrarTejido(str(r.db_tejido)).conn.close()

    origen = getTransacciones(r.db_hilado, r.db_tejido)
    resumen = TransformResumen(origen, COD_DICT).getResumen()

    m = MigrarResumen(str(r.db_resumen))
    m.conn.execute("DELETE FROM resumen_op")
    m.conn.commit()
    m.migrar_resumen(resumen)
    m.conn.close()

    filtrado = origen[(origen["kgs"] != 0) & (origen["dolares"] != 0)]
    usd_origen, usd_resumen = _num(filtrado, "dolares"), _num(resumen, "valorizado_total")
    if abs(usd_origen - usd_resumen) > max(1.0, abs(usd_origen) * 1e-6):
        raise CalidadError(f"Conciliación USD fallida: origen {usd_origen:,.2f} vs resumen {usd_resumen:,.2f}")

    pu = pd.to_numeric(resumen["precio_unitario_ponderado"], errors="coerce").astype("float64")
    pu_invalidos = int((~np.isfinite(pu)).sum())
    if pu_invalidos:
        log.warning("%d filas con precio unitario ponderado inválido", pu_invalidos)

    return {
        "transacciones_origen": int(len(origen)),
        "filas_resumen": int(len(resumen)),
        "ops": int(resumen["op"].nunique()),
        "usd_origen": usd_origen,
        "usd_resumen": usd_resumen,
        "precio_unitario_invalidos": pu_invalidos,
        "por_categoria": _desglose(resumen, "categoria", kgs="kg_netos", usd="valorizado_total", top=20),
    }


def publicar_datos_dashboard() -> dict:
    """Deja un indicador de frescura y el top de OPs para el dashboard."""
    with closing(sqlite3.connect(rutas().db_resumen)) as conn, conn:
        top = pd.read_sql_query(
            """SELECT op, ROUND(SUM(kg_netos),2) AS kg_netos, ROUND(SUM(valorizado_total),2) AS valorizado_total
               FROM resumen_op GROUP BY op ORDER BY valorizado_total DESC LIMIT 10""",
            conn,
        )
        conn.execute("CREATE TABLE IF NOT EXISTS ultima_actualizacion (actualizado_en TEXT NOT NULL)")
        conn.execute("DELETE FROM ultima_actualizacion")
        conn.execute("INSERT INTO ultima_actualizacion VALUES (datetime('now','localtime'))")
    log.info("Top 10 OPs por valorizado:\n%s", top.to_string(index=False))
    return {"top_10_ops": top.to_dict(orient="records")}
