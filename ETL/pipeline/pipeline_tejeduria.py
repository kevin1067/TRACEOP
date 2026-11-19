from ETL.EXTRACT.extract_hilado import getExcelMovData
from config.config import(
    COLS_MOV_TEJEDURIA, ASPECTO_MOV_TEJEDURIA,
    COLS_ALM_TEJEDURIA, COD_DICT)

from ETL.TRANSFORM.transform_alm_tejeduria import TransformTejido
from ETL.LOAD.load_tejido import MigrarTejido
from pathlib import Path

def PipelineTejeduria(year:int,tejido_path:Path,generate_excel:bool) -> None:
    #Etapa de extraccion de informacion
    print("\n Iniciando pipeline de tejeduria...")
    df = getExcelMovData(tejido_path)
    transform_tejido = TransformTejido(df,COD_DICT)
    df_alm_tejido = transform_tejido.getAlmTejeduria(COLS_ALM_TEJEDURIA)
    df_mov_tejido = transform_tejido.getMovTejeduria(year,ASPECTO_MOV_TEJEDURIA,COLS_MOV_TEJEDURIA)
    
    if generate_excel:
        df_alm_tejido.to_excel(f"reports/tejeduria/codigos_tejido {year}.xlsx",index=False) 
        df_mov_tejido.to_excel(f"reports/tejeduria/movimientos_tejido {year}.xlsx",index=False)
    else:
        migrador = MigrarTejido("migracion/control_tejidos.db")
        migrador.migrar_catologo_tejeduria(df_alm_tejido)
        migrador.migrar_movimientos_tejeduria(df_mov_tejido)