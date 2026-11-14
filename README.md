# crear un .env en el proyecto:

TESTING_PATH="C:\\Users\\{tu nombre de usuario}\\Desktop\\TRACEOP\\datos_excel" #ruta local para pruebas de extraccion y migración de datos 

DB_PRODUCTION_PATH="C:\\Users\\{tu nombre de usuario}\\Desktop\\TRACEOP\\migracion" #esta ruta será la de cloud sql (gcp)

DB_TESTING_PATH="C:\\Users\\{tu nombre de usuario}\\Desktop\\TRACEOP\\migracion"

- Crea una carpeta "migracion" en la raiz del proyecto
- Crea una carpeta "datos_excel" en la raiz del proyecto, dentro de la carpeta crea una carpeta con el año 2026, en esa carpeta subes los datos de enero a junio de movimientos.
- Crea un entorno virtual y activalo
- Una vez activado, instala las librerias necesarias para iniciar el ETL:

```bash
pip install -r requirements.txt
```

- Una vez instalado ejecutar el script:

```bash
py main.py
```

---

# Orquestador del ETL (Dagster)

El ETL se orquesta con [Dagster](https://dagster.io). Cada etapa se modela como un **asset**, es decir, un dato producido, con linaje, metadatos, verificaciones de calidad e historial de ejecuciones, y todo se puede ver en una interfaz web local.

## Ejecutar

```powershell
pip install -r requirements.txt
.\orquestador.ps1        # o doble clic en orquestador.bat
```

Abrir **http://localhost:3000**. Usa las mismas variables del `.env` (`TESTING_PATH`, `DB_TESTING_PATH`).

## Grafo de linaje

```
                         ┌─► movimientos_hilado ────► sqlite_mov_hilado ─────┐
                         ├─► consumo_hilado ────────► sqlite_consumo_hilado ─┤
excel_trazabilidad ──────┤                                                   ├─► resumen_op ─► dashboard_powerbi
     _partidas           ├─► catalogo_tejeduria ────► sqlite_alm_tejeduria   │
                         └─► movimientos_tejeduria ─► sqlite_mov_tejeduria ──┘
   [extraccion]               [transformacion]            [carga_sqlite]          [analitica]
```

| Grupo | Assets | Equivale a |
|---|---|---|
| `extraccion` | `excel_trazabilidad_partidas` | `getExcelMovData` |
| `transformacion` | `movimientos_hilado`, `consumo_hilado`, `catalogo_tejeduria`, `movimientos_tejeduria` | `TransformHilado`, `TransformTejido` |
| `carga_sqlite` | `sqlite_*` | `MigrarHilado`, `MigrarTejido` |
| `analitica` | `resumen_op`, `dashboard_powerbi` (externo) | `PipelineResumen` |

- **Particiones por año**: reemplazan la pregunta interactiva `prompt_year()`. En la UI se elige el año, o varios con un *backfill*.
- **Cargas idempotentes**: al volver a ejecutar un año se reemplazan sus filas (`anio_registro`) en lugar de duplicarlas. `resumen_op` se recalcula completo.
- **Verificaciones de calidad (asset checks)**: columnas requeridas (bloqueante), OP informada, tipo de movimiento ∈ {E,S}, categoría mapeada en `COD_DICT`, duplicados, conciliación de filas Excel → SQLite y conciliación de USD en el resumen.
- **Metadatos en cada asset**: filas, esquema de columnas, vista previa, totales de kg y USD, desglose por categoría y top 10 de OPs por valorizado.

## Jobs y automatización

| Nombre | Tipo | Descripción |
|---|---|---|
| `etl_traceop` | Job | ETL completo de un año |
| `etl_hilado` / `etl_tejeduria` | Job | Solo una cadena |
| `recalcular_resumen` | Job | Solo `resumen_op` |
| `cierre_mensual` | Schedule | Día 1 de cada mes, 06:00 (Lima) |
| `nuevos_excel` | Sensor | Lanza el ETL cuando aparece o cambia un Excel en `datos_excel/<año>` |

Los schedules y sensores vienen **apagados** y se encienden desde *Automation*.

## Guion sugerido para la demo

1. **Assets → View lineage**: recorrer el grafo por grupos (Excel → pandas → SQLite → Power BI).
2. **Materialize all**, elegir la partición `2026` y seguir la ejecución en vivo en la vista *Run* (diagrama de Gantt por etapa y logs).
3. Abrir `movimientos_hilado` → *Overview*: vista previa, esquema y desglose por categoría.
4. Pestaña **Checks**: mostrar las reglas de calidad y la conciliación Excel ↔ SQLite.
5. Abrir `resumen_op`: top 10 de OPs y conciliación de USD.
6. **Automation**: activar el sensor `nuevos_excel`, copiar un Excel a `datos_excel/2026` y ver cómo se dispara solo.
