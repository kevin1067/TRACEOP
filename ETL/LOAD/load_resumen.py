import pandas as pd
import sqlite3

class MigrarResumen:
    def __init__(self,db_name):
        """Inicializa la conexión, el cursor y configura la base de datos."""
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        
        # Activar el soporte de claves foráneas
        self.cursor.execute("PRAGMA foreign_keys = ON;")
        self.conn.commit()
        
        # Crear las tablas al instanciar la clase si no existen
        self.crear_tablas()

    def crear_tablas(self) -> None:
        """Crea la estructura de la base de datos."""

        self.cursor.execute("PRAGMA foreign_keys = ON;")

        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS resumen_op (
            op                          VARCHAR(10)  NOT NULL,
            categoria                   VARCHAR(15)  NOT NULL,
            kg_netos                    DECIMAL(12,2) NOT NULL DEFAULT 0,
            valorizado_total            DECIMAL(12,2) NOT NULL DEFAULT 0,
            precio_unitario_ponderado   DECIMAL(12,2) NOT NULL DEFAULT 0
        );
        ''')
        self.conn.commit()


    def migrar_resumen(self,df:pd.DataFrame):
        df.to_sql("resumen_op",self.conn,if_exists='append', index=False)
        print("¡Resumen por migrado con éxito!")