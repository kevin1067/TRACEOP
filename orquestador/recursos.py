"""Recursos compartidos: rutas de datos y bases SQLite, particiones por año."""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import dagster as dg
from dotenv import load_dotenv

load_dotenv()

# Los prints del ETL usan ✔/✘; la consola de Windows (cp1252) no los soporta.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# Particiones por año: mismo rango que ofrecía `prompt_year()` en main.py
_ANIO_ACTUAL = datetime.now().year
particiones_anio = dg.StaticPartitionsDefinition(
    [str(a) for a in range(_ANIO_ACTUAL - 4, _ANIO_ACTUAL + 1)]
)


class RutasTraceOp(dg.ConfigurableResource):
    """Ubicación de los Excel de origen y de las bases SQLite de destino.

    Se alimenta de las mismas variables del `.env` que usa `main.py`
    (TESTING_PATH / DB_TESTING_PATH).
    """

    datos_path: str
    db_path: str

    def carpeta_anio(self, anio: str | int) -> Path:
        return Path(self.datos_path) / str(anio)

    def db(self, nombre: str) -> Path:
        ruta = Path(self.db_path)
        ruta.mkdir(parents=True, exist_ok=True)
        return ruta / nombre

    @property
    def db_hilado(self) -> Path:
        return self.db("control_hilados.db")

    @property
    def db_tejido(self) -> Path:
        return self.db("control_tejidos.db")

    @property
    def db_resumen(self) -> Path:
        return self.db("resumen_op.db")

