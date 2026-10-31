import pandas as pd
import os
from pathlib import Path
import sqlite3

def getExcelMovData(file_path: Path) -> pd.DataFrame:
    print(file_path)
    try:
        existe = os.path.exists(file_path)
    except Exception as e:
        raise ValueError(f"Error al verificar la existencia de la ruta {file_path}: {e}") from e
 
    if not existe:
        raise FileNotFoundError(f"La ruta {file_path} no existe.")
 
    excel_list = os.listdir(file_path)
    dfs = []
    count = 0
 
    for f in excel_list:
        ruta = os.path.join(file_path, f)
        if not os.path.isfile(ruta) or not f.lower().endswith((".xlsx", ".xlsm")):
            continue
        try:
            df = pd.read_excel(ruta)
            dfs.append(df)
            count += 1
            print(f"  ✔ Leído: {f} ({len(df)} filas)")
        except Exception as e:
            print(f"  ✘ Error procesando {f}: {e}")
 
    if not dfs:
        raise ValueError(f"No se encontraron archivos Excel válidos en {file_path}.")
 
    result = pd.concat(dfs, ignore_index=True)
    print(f"\nProceso finalizado. {count} archivo(s) leídos — {len(result)} filas en total.")
    #print(result.head())
    return result


def getTransacciones(path_db_hilado:Path,path_db_tejeduria:Path) -> pd.DataFrame:
    # 1. Extracción de datos de la base de datos de Hilado
    conn_hilado = sqlite3.connect(path_db_hilado)
    # Se asume que ambas tablas están en este archivo db
    query_mov_hilado = """
        SELECT op, transaccion, categoria,kgs, dolares 
        FROM mov_hilado
    """
    df_mov_hilado = pd.read_sql_query(query_mov_hilado, conn_hilado)
    
    query_cons_hilado = """
        SELECT op, transaccion,categoria, kgs, dolares
        FROM consumo_hilado
    """
    df_cons_hilado = pd.read_sql_query(query_cons_hilado, conn_hilado)
    
    conn_hilado.close()

    # 2. Extracción de datos de la base de datos de Tejeduría
    conn_tejeduria = sqlite3.connect(path_db_tejeduria)
    
    query_tejeduria = """
        SELECT op, transaccion, categoria,kgs, dolares 
        FROM mov_tejeduria
    """
    df_tejeduria = pd.read_sql_query(query_tejeduria, conn_tejeduria)
    
    conn_tejeduria.close()

    df_consolidado = pd.concat(
        [df_mov_hilado, df_cons_hilado, df_tejeduria], 
        ignore_index=True
    )
    return df_consolidado