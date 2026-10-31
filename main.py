from ETL.pipeline.pipeline_resumen import PipelineResumen
from ETL.pipeline.pipeline_hilado import PipelineHilado
from ETL.pipeline.pipeline_tejeduria import PipelineTejeduria
from pathlib import Path
from utils.utils import prompt_year, setup_environment_variables


def main():
    
    #Variables
    
    main_path, migration_path = setup_environment_variables("t")
    DB_MIGRACION = Path(migration_path)
    MAIN_PATH = Path(main_path)
    db_hilado = DB_MIGRACION / "control_hilados.db"
    db_tejido = DB_MIGRACION / "control_tejidos.db"
    year = prompt_year()
    
    hilado_path_year = MAIN_PATH / year

    #Pipeline de hilado: Extracción->Transformación->Carga de datos a sqlite
    #PipelineHilado(int(year),hilado_path_year,False)
    #PipelineTejeduria(int(year),hilado_path_year,False)
    PipelineResumen(db_hilado,db_tejido,False)



if __name__ == "__main__":
    print("iniciando con el ETL de trazabilidad...")
    main()