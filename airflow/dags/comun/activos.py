"""Assets de Airflow de TraceOP: representan las tablas que producen los DAGs.

Permiten la programación por datos (el resumen se ejecuta cuando se cargan los
movimientos) y la vista de linaje en la pestaña *Assets* de la UI.
"""

from airflow.sdk import Asset

MOV_HILADO = Asset("traceop_mov_hilado", extra={"db": "control_hilados.db", "tabla": "mov_hilado"})
CONSUMO_HILADO = Asset("traceop_consumo_hilado", extra={"db": "control_hilados.db", "tabla": "consumo_hilado"})
ALM_TEJEDURIA = Asset("traceop_alm_tejeduria", extra={"db": "control_tejidos.db", "tabla": "alm_tejeduria"})
MOV_TEJEDURIA = Asset("traceop_mov_tejeduria", extra={"db": "control_tejidos.db", "tabla": "mov_tejeduria"})
RESUMEN_OP = Asset("traceop_resumen_op", extra={"db": "resumen_op.db", "tabla": "resumen_op"})
DATOS_DASHBOARD = Asset(
    "traceop_datos_dashboard",
    extra={"descripcion": "Datos de trazabilidad de costos por OP listos para el dashboard"},
)
