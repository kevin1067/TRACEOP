import pandas as pd
import sqlite3

class MigrarTejido:
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
        CREATE TABLE IF NOT EXISTS alm_tejeduria (
            partida   VARCHAR(20)  PRIMARY KEY,
            cod_tela  VARCHAR(15)  NOT NULL,
            des_tela  VARCHAR(150) NOT NULL,
            des_comb  VARCHAR(150),
            cod_color VARCHAR(15)  NOT NULL,
            des_color VARCHAR(150) NOT NULL
        );
        ''')

        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS mov_tejeduria (
            id_mov       INTEGER PRIMARY KEY AUTOINCREMENT,
            op           VARCHAR(10)   NOT NULL,
            partida      VARCHAR(20)   NOT NULL REFERENCES alm_tejeduria(partida),
            cod_tela     VARCHAR(15),
            des_tela     VARCHAR(150),
            des_comb     VARCHAR(150),   -- ex "combo"
            cod_color    VARCHAR(15),
            des_color    VARCHAR(150),
            aspecto      VARCHAR(100),
            categoria    VARCHAR(20),
            transaccion  VARCHAR(100),
            tipo_mov     CHAR(1)       NOT NULL CHECK (tipo_mov IN ('E','S')),
            cliente      VARCHAR(20),
            proveedor    VARCHAR(100),
            fecha        DATE          NOT NULL,
            guia         VARCHAR(30),
            oc           VARCHAR(30),
            factura      VARCHAR(30),
            kgs          DECIMAL(12,3) NOT NULL DEFAULT 0,
            dolares      DECIMAL(12,2) NOT NULL DEFAULT 0,
            cu_usd       DECIMAL(10,4) NOT NULL DEFAULT 0,
            cu_usd_calc  DECIMAL(10,4) NOT NULL DEFAULT 0,
            anio_registro  INTEGER NOT NULL
        );
        ''')

        self.conn.commit()

    def migrar_catologo_tejeduria(self, df_almacen_unico:pd.DataFrame):

        query = "SELECT partida FROM alm_tejeduria;"
        df_existentes = pd.read_sql_query(query,self.conn)
        lista_existentes = df_existentes['partida'].tolist()

        df_almacen_unico = df_almacen_unico.rename(columns= {
            "Partida" : "partida",
            "Cod_Tela" : "cod_tela",
            "Des_Tela": "des_tela",
            "Des_Comb": "des_comb",
            "Cod_Color": "cod_color",
            "Des_Color": "des_color"
        })

        df_nueva_tela = df_almacen_unico[~df_almacen_unico['partida'].isin(lista_existentes)]
        # Eliminar duplicados dentro del mismo archivo (por precaución)
        df_nueva_tela = df_nueva_tela.drop_duplicates(subset=['partida'])

        if not df_nueva_tela.empty:
            df_nueva_tela.to_sql('alm_tejeduria', self.conn, if_exists='append', index=False)
            print(f"Se insertaron {len(df_nueva_tela)} nuevas partidas de tela.")
        else:
            print("No se encontraron nuevas partidas de tela en este archivo.")

    def migrar_movimientos_tejeduria(self,df_mov_tejido:pd.DataFrame)->None:
        df_movimientos_sql = df_mov_tejido.rename(columns={
            "OP Referencial":"op",
            "Partida": "partida",
            "Cod_Tela":"cod_tela",
            "Des_Tela":"des_tela",
            "Des_Comb":"des_comb",
            "Cod_Color":"cod_color",
            "Des_Color":"des_color",
            "Aspecto":"aspecto",
            "Tipo":"tipo_mov",
            "Transaccion": "transaccion",
            "Cliente":"cliente",
            "Proveedor": "proveedor",
            "Fecha": "fecha",
            "Guia": "guia",
            "OC": "oc",
            "Factura": "factura",
            "Kgs": "kgs",
            "dolares": "dolares",
            "cu_usd": "cu_usd",
        })

        columnas_movimiento=[
            "op","partida","cod_tela","des_tela","des_comb","cod_color","des_color",
            "aspecto","tipo_mov","transaccion",'categoria',"cliente","proveedor","fecha","guia","oc","factura",
            "kgs","dolares","cu_usd",'cu_usd_calc','anio_registro'
        ]
        df_movimientos_sql = df_movimientos_sql[columnas_movimiento]
        df_movimientos_sql['fecha'] = pd.to_datetime(df_movimientos_sql['fecha']).dt.strftime('%Y-%m-%d')

        df_movimientos_sql.to_sql('mov_tejeduria', self.conn, if_exists='append', index=False)
        print("¡Movimientos de tejeduria migrados con éxito!")

    