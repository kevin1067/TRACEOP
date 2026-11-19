# TraceOP: prototipo del ETL de trazabilidad de costos por OP

TraceOP consolida los movimientos de **hilado** y **tejeduría** de cada Orden de Producción (OP) y calcula su **costo total y su precio unitario ponderado por categoría**. Los datos de origen son los reportes mensuales de trazabilidad de partidas (`rptTrazabilidadPartidas`) exportados del ERP en Excel. El destino son bases SQLite listas para el dashboard.

Este repositorio contiene el **prototipo funcional** del ETL, que se ejecuta de forma local con `main.py`.

> La orquestación con Apache Airflow está planificada como **implementación futura** y se documenta por separado en [airflow/README.md](airflow/README.md). Este prototipo no necesita Airflow ni Docker.

---

## Flujo del ETL

```
 datos_excel/<año>/*.xlsx
          │
          ▼
 ┌─────────────────── PipelineHilado ───────────────────┐
 │ Extracción → Transformación → Carga                  │──► migracion/control_hilados.db
 │ (movimientos y consumo/merma de hilado)              │      ├─ alm_hilado
 └──────────────────────────────────────────────────────┘      ├─ mov_hilado
          │                                                    └─ consumo_hilado
          ▼
 ┌────────────────── PipelineTejeduria ─────────────────┐
 │ Extracción → Transformación → Carga                  │──► migracion/control_tejidos.db
 │ (partidas de tela y movimientos de tejeduría,        │      ├─ alm_tejeduria
 │  tintorería, post-teñido y confección)               │      └─ mov_tejeduria
 └──────────────────────────────────────────────────────┘
          │
          ▼
 ┌─────────────────── PipelineResumen ──────────────────┐
 │ Lee ambas bases → agrupa por OP × categoría          │──► migracion/resumen_op.db
 │ (kg netos, valorizado USD, precio unitario ponderado)│      └─ resumen_op
 └──────────────────────────────────────────────────────┘
          │
          ▼
      Dashboard
```

El orden importa: **PipelineResumen** lee las tablas que cargan los otros dos pipelines, así que va al final.

## Estructura del proyecto

```
TRACEOP/
├── main.py                      # punto de entrada del prototipo
├── config/config.py             # columnas, aspectos y diccionario de códigos de transacción
├── utils/utils.py               # selección del año y lectura del .env
├── ETL/
│   ├── EXTRACT/extract_hilado.py        # lectura de Excel y de las bases SQLite
│   ├── TRANSFORM/
│   │   ├── transform_alm_hilado.py      # reglas de negocio de hilado
│   │   ├── transform_alm_tejeduria.py   # reglas de negocio de tejeduría
│   │   └── transform_resumen.py         # agregación por OP × categoría
│   ├── LOAD/
│   │   ├── load_hilado.py               # control_hilados.db
│   │   ├── load_tejido.py               # control_tejidos.db
│   │   └── load_resumen.py              # resumen_op.db
│   └── pipeline/
│       ├── pipeline_hilado.py
│       ├── pipeline_tejeduria.py
│       └── pipeline_resumen.py
├── datos_excel/<año>/           # reportes Excel de entrada (no se versiona)
├── migracion/                   # bases SQLite de salida (no se versiona)
└── reports/                     # Excel de revisión, opcionales (no se versiona)
```

## Requisitos

- Windows 10/11 o macOS (con Apple Silicon o Intel).
- Python **3.11 o superior** (pandas 3 no soporta versiones anteriores).
- Las dependencias de [requirements.txt](requirements.txt): `pandas`, `openpyxl`, `python-dotenv` y `questionary`.

### Instalar Python

**Windows:** descarga el instalador desde [python.org](https://www.python.org/downloads/) y marca *Add python.exe to PATH*. Comprueba la versión con `py --version`.

**macOS:** el Python que trae el sistema suele ser una versión antigua. Instala uno nuevo con [Homebrew](https://brew.sh) o con el instalador de [python.org](https://www.python.org/downloads/macos/):

```bash
brew install python@3.13
python3 --version        # debe mostrar 3.11 o superior
```

> En macOS no existe el comando `py`. Usa `python3`, o `python` una vez activado el entorno virtual.

## Instalación

**1. Crear las carpetas de trabajo** en la raíz del proyecto:

```
datos_excel/2026/      ← aquí van los reportes mensuales (por ejemplo, enero a junio)
migracion/
reports/hilado/        ← solo si se van a generar Excel de revisión
reports/tejeduria/
reports/resumen/
```

En macOS puedes crearlas desde la Terminal, ubicado en la raíz del proyecto:

```bash
mkdir -p datos_excel/2026 migracion reports/hilado reports/tejeduria reports/resumen
```

**2. Crear el archivo `.env`** en la raíz, con las rutas de tu equipo.

**Windows:** la barra invertida va duplicada (`\\`).

```env
TESTING_PATH="C:\\Users\\<usuario>\\Desktop\\TRACEOP\\datos_excel"
DB_TESTING_PATH="C:\\Users\\<usuario>\\Desktop\\TRACEOP\\migracion"

# Reservadas para el entorno productivo (Cloud SQL en GCP)
PRODUCTION_PATH=""
DB_PRODUCTION_PATH=""
```

**macOS:** usa barras normales (`/`) y la ruta completa. `~` no se expande dentro del `.env`.

```env
TESTING_PATH="/Users/<usuario>/Desktop/TRACEOP/datos_excel"
DB_TESTING_PATH="/Users/<usuario>/Desktop/TRACEOP/migracion"

# Reservadas para el entorno productivo (Cloud SQL en GCP)
PRODUCTION_PATH=""
DB_PRODUCTION_PATH=""
```

Para obtener la ruta exacta en macOS, ejecuta `pwd` en la Terminal estando en la carpeta del proyecto.

`main.py` usa el entorno de pruebas (`"t"`): lee los Excel desde `TESTING_PATH` y las bases desde `DB_TESTING_PATH`.

**3. Crear el entorno virtual e instalar las dependencias.**

**Windows** (PowerShell):

```powershell
py -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Si PowerShell bloquea la activación, ejecuta una vez `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`.

**macOS** (Terminal):

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Con el entorno activo, el prompt muestra `(venv)` al inicio. Para salir del entorno: `deactivate`.

## Ejecución

Desde la **raíz del proyecto** y con el entorno virtual activo, porque las cargas escriben en `migracion/` con una ruta relativa:

**Windows:**

```powershell
py main.py
```

**macOS:**

```bash
python3 main.py
```

El script pide elegir el **año a procesar** (el año en curso y los cuatro anteriores) y busca los reportes en `datos_excel/<año>/`. El selector se maneja con las flechas ↑ ↓ y Enter, tanto en PowerShell como en la Terminal de macOS.

### Qué se ejecuta

Los pipelines se activan o desactivan comentando sus líneas en [main.py](main.py):

```python
PipelineHilado(int(year), hilado_path_year, False)
PipelineTejeduria(int(year), hilado_path_year, False)
PipelineResumen(db_hilado, db_tejido, False)
```

El último parámetro de cada pipeline (`generate_excel`) define el destino:

| Valor | Resultado |
|---|---|
| `False` | Carga los datos en las bases SQLite de `migracion/` |
| `True` | Genera Excel de revisión en `reports/` y no toca las bases |

El modo `True` sirve para revisar la transformación en Excel antes de cargarla.

## Detalle de cada pipeline

### PipelineHilado → `control_hilados.db`

| Etapa | Qué hace |
|---|---|
| Extracción | Une todos los `.xlsx` / `.xlsm` de la carpeta del año en un solo DataFrame |
| Transformación (movimientos) | Filtra los aspectos de `ASPECTO_MOV_HILADO`, normaliza la OP, propaga la OC en las transacciones **Y18**, invierte el signo de **X18** (devoluciones), elimina duplicados por OP + N.º de movimiento + kg y recalcula el costo unitario del teñido (**X34 / L34**) sobre los kg despachados |
| Transformación (consumo) | Filtra *Hilado Consumido* y *Hilado Merma* y los marca como **JE4C** / **JE4M** |
| Carga | `mov_hilado` y `consumo_hilado`. Los códigos de hilado que no existen en el catálogo se agregan a `alm_hilado` como `SIN DESCRIPCION` para respetar la clave foránea |

### PipelineTejeduria → `control_tejidos.db`

| Etapa | Qué hace |
|---|---|
| Transformación (catálogo) | Partidas únicas de tela con su tela, combinación y color |
| Transformación (movimientos) | Filtra los aspectos de `ASPECTO_MOV_TEJEDURIA`, elimina duplicados en **TCM**, marca la *Tela principal* (códigos de tela que empiezan con `C`) y calcula el costo unitario de tintorería **T21** sobre los kg enviados en **JS8** |
| Carga | `alm_tejeduria` (solo partidas nuevas) y `mov_tejeduria` |

### PipelineResumen → `resumen_op.db`

Une `mov_hilado`, `consumo_hilado` y `mov_tejeduria`, descarta las filas con kg o USD en cero y agrupa por **OP × categoría**:

| Columna | Cálculo |
|---|---|
| `kg_netos` | Suma de kg |
| `valorizado_total` | Suma de USD |
| `precio_unitario_ponderado` | `valorizado_total / kg_netos` (0 si no hay kg) |

## Configuración de negocio

Todas las reglas parametrizables están en [config/config.py](config/config.py):

- **`ASPECTO_*`**: los aspectos del reporte que entran a cada pipeline.
- **`COLS_*`**: las columnas que se toman del Excel.
- **`COD_DICT`**: la categoría de cada código de transacción (por ejemplo, `X34` → *Servicio teñido de hilado*).

Si aparece en el Excel un código de transacción que no está en `COD_DICT`, la transformación se detiene y muestra cuáles son los códigos nuevos. Hay que agregarlos al diccionario con su categoría y volver a ejecutar.

## Resultados de referencia

Procesamiento del año 2026 (6 reportes, de enero a junio):

| Dato | Filas |
|---|---|
| Filas leídas de los Excel | 282 405 |
| `mov_hilado` | 30 127 |
| `consumo_hilado` | 18 853 |
| `alm_tejeduria` (partidas) | 4 777 |
| `mov_tejeduria` | 34 338 |
| `resumen_op` | 7 034 (1 487 OPs) |

La lectura de los Excel es la etapa más lenta (unos 30 segundos). Las transformaciones y las cargas tardan pocos segundos.

## Problemas frecuentes

| Síntoma | Causa y solución |
|---|---|
| `command not found: python` o `py` (macOS) | En macOS el comando es `python3`. Dentro del entorno virtual activo también funciona `python`. |
| `pandas` no se instala o pide Python más nuevo | La versión de Python es anterior a la 3.11. Instala una versión reciente (ver *Instalar Python*) y vuelve a crear el `venv`. |
| `The environment variable ... is not set` | Falta el archivo `.env` en la raíz del proyecto o le falta una variable. En macOS, revisa que el archivo se llame exactamente `.env`: TextEdit puede agregarle `.txt` (los archivos que empiezan con punto se ven con `Cmd + Shift + .` en Finder). |
| `La ruta ... no existe` | `TESTING_PATH` no apunta a `datos_excel`, o falta la subcarpeta del año elegido. |
| `✘ Error procesando ~$archivo.xlsx` | Ese Excel está abierto. Ciérralo: Office crea un archivo temporal `~$` mientras el archivo está en uso. |
| `No such file or directory: 'reports/...'` | Se usa `generate_excel=True` sin haber creado las carpetas `reports/hilado`, `reports/tejeduria` y `reports/resumen`. |
| `Se encontraron códigos no configurados` | Hay un código de transacción nuevo en el Excel. Agrégalo a `COD_DICT` en `config/config.py`. |

## Limitaciones del prototipo

- **Las cargas solo agregan filas:** si se ejecuta dos veces el mismo año, los movimientos y el resumen se duplican. Para reprocesar, hay que borrar las bases de `migracion/` (o las filas de ese año) antes de volver a ejecutar.
- **Ejecución manual:** el año se elige por consola y los pipelines se activan comentando líneas de `main.py`.
- **Sin validaciones automáticas de calidad** más allá de la verificación de códigos de transacción.
- **Rutas mixtas:** la extracción usa las rutas del `.env`, pero las cargas escriben en `migracion/` relativo a la raíz, así que `DB_TESTING_PATH` debe apuntar a esa misma carpeta.

## Implementación futura

La siguiente etapa resuelve estas limitaciones con **Apache Airflow**: ejecución programada y por parámetros, cargas idempotentes por año, reglas de calidad de datos y monitoreo visual de cada etapa. El diseño está en [airflow/README.md](airflow/README.md).
