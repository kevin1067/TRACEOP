from ETL.EXTRACT.extract_hilado import getTransacciones

from config.config import COD_DICT

from ETL.TRANSFORM.transform_resumen import TransformResumen
from ETL.LOAD.load_resumen import MigrarResumen

from pathlib import Path
def PipelineResumen(hilado_path:Path,tejeduria_path:Path,generate_excel:bool) -> None:
    
    #Etapa de extraccion de informacion
    df = getTransacciones(hilado_path,tejeduria_path)

    #Etapa de transformación de información de hilado
    transform_resumen = TransformResumen(df,COD_DICT)
    resumen = transform_resumen.getResumen()

    #En caso generate_report sea true se generaran excel, en caso sea falso, se procedera con la migracion
    if generate_excel:
        
        resumen.to_excel(f"reports/resumen/resumen.xlsx",index=False)
        #df_consumo_hilado.to_excel(f"reports/hilado/consumo_hilado.xlsx",index=False)
    
    else:
        
        migrador = MigrarResumen('migracion/resumen_op.db')
        #Ejecutar las migraciones pasando tus DataFrames procesados
        migrador.migrar_resumen(resumen)