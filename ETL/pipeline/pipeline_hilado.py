from ETL.EXTRACT.extract_hilado import getExcelMovData

from config.config import (
    COLS_MOV_HILADO, ASPECTO_MOV_HILADO,
    COLS_ALM_HILADO, ASPECTO_CONSUMO_HILADO,
    COLS_CONSUMO_HILADO,COD_DICT)

from ETL.TRANSFORM.transform_alm_hilado import TransformHilado
from ETL.LOAD.load_hilado import MigrarHilado

from pathlib import Path
def PipelineHilado(year:int,hilado_path:Path,generate_excel:bool) -> None:

    print("\n Iniciando el pipeline de hilado...")
    #Etapa de extraccion de informacion
    df = getExcelMovData(hilado_path)

    #Etapa de transformación de información de hilado
    transform_hilado = TransformHilado(df,COD_DICT)
    #df_alm_hilado = transform_hilado.getAlmHilado(COLS_ALM_HILADO)
    df_movs_hilado = transform_hilado.getMovHilado(year,ASPECTO_MOV_HILADO,COLS_MOV_HILADO)
    df_consumo_hilado = transform_hilado.getMovConsumoHilado(year,ASPECTO_CONSUMO_HILADO,COLS_CONSUMO_HILADO)

    #En caso generate_report sea true se generaran excel, en caso sea falso, se procedera con la migracion
    if generate_excel:
        #a = ""
        #df_alm_hilado.to_excel(f"reports/hilado/maestro_codigos {year}.xlsx",index=False)
        df_movs_hilado.to_excel(f"reports/hilado/movimientos_hilado {year}.xlsx",index=False)
        df_consumo_hilado.to_excel(f"reports/hilado/consumo_hilado.xlsx",index=False)
    
    else:
        
        migrador = MigrarHilado('migracion/control_hilados.db')
        #Ejecutar las migraciones pasando tus DataFrames procesados
        #migrador.migrar_catalogo(df_alm_hilado)
        migrador.migrar_movimientos(df_movs_hilado)
        migrador.migrar_consumos(df_consumo_hilado)