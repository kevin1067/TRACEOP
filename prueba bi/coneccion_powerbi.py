import sqlite3
import pandas as pd

# Conexión a SQLite
conn = sqlite3.connect(r"C:\Users\kzevallos\Desktop\Implementaciones\TRAZABILIDAD-OPS\control_hilados.db")
# Cargar tabla
dataset = pd.read_sql_query("""
    SELECT *
    FROM mov_hilado
    """, conn)

conn.close()