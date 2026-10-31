import pandas as pd
import sqlite3





class MigrarHilado:
    CODIGOS_INVALIDOS = ['nan', 'None', '', '<NA>']
 
    def __init__(self, db_name='control_hilados.db'):
        """Inicializa la conexión, el cursor y configura la base de datos."""
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
 
        # Activar el soporte de claves foráneas (es por conexión, no persistente)
        self.cursor.execute("PRAGMA foreign_keys = ON;")
        self.conn.commit()
 
        # Crear las tablas al instanciar la clase si no existen
        self.crear_tablas()
 
    # ------------------------------------------------------------------
    # Estructura de la base de datos
    # ------------------------------------------------------------------
    def crear_tablas(self):
        """Crea la estructura de la base de datos."""
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS alm_hilado (
            cod_hilado  VARCHAR(20)  PRIMARY KEY,
            descripcion VARCHAR(200) NOT NULL
        );
        ''')
 
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS mov_hilado (
            id_mov        INTEGER PRIMARY KEY AUTOINCREMENT,
            cod_hilado    VARCHAR(20)   NOT NULL,
            op            VARCHAR(10)   NOT NULL,
            fecha         DATE          NOT NULL,
            cliente       VARCHAR(20)   NOT NULL,
            tipo_mov      CHAR(1)       NOT NULL CHECK (tipo_mov IN ('E','S')),
            aspecto       VARCHAR(100),
            categoria     VARCHAR(20),
            codigo        VARCHAR(4),
            proveedor     VARCHAR(100),
            transaccion   VARCHAR(100),
            num_mov       VARCHAR(20),
            guia          VARCHAR(30),
            oc            VARCHAR(30),
            factura       VARCHAR(30),
            kgs           DECIMAL(12,3) DEFAULT 0,
            dolares       DECIMAL(12,2) DEFAULT 0,
            cu_usd        DECIMAL(10,4) DEFAULT 0,
            cu_usd_calc   DECIMAL(10,4) DEFAULT 0,
            soles         DECIMAL(12,2) DEFAULT 0,
            cu_soles      DECIMAL(10,4) DEFAULT 0,
            anio_registro INTEGER       NOT NULL,
            FOREIGN KEY (cod_hilado) REFERENCES alm_hilado(cod_hilado)
                ON UPDATE CASCADE ON DELETE RESTRICT
        );
        ''')
        self.cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_mov_hilado_cod ON mov_hilado(cod_hilado);")
 
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS consumo_hilado (
            id_consumo    INTEGER PRIMARY KEY AUTOINCREMENT,
            cod_hilado    VARCHAR(20)   NOT NULL,
            op            VARCHAR(10)   NOT NULL,
            fecha         DATE          NOT NULL,
            cliente       VARCHAR(20)   NOT NULL,
            tipo_mov      CHAR(1)       NOT NULL CHECK (tipo_mov IN ('E','S')),
            aspecto       VARCHAR(100),
            proveedor     VARCHAR(100),
            transaccion   VARCHAR(100),
            num_mov       VARCHAR(20),
            categoria     VARCHAR(20),
            codigo        VARCHAR(4),
            guia          VARCHAR(30),
            oc            VARCHAR(30),
            factura       VARCHAR(30),
            kgs           DECIMAL(12,3) DEFAULT 0,
            dolares       DECIMAL(12,2) DEFAULT 0,
            cu_usd        DECIMAL(10,4) DEFAULT 0,
            cu_usd_calc   DECIMAL(10,4) DEFAULT 0,
            soles         DECIMAL(12,2) DEFAULT 0,
            cu_soles      DECIMAL(10,4) DEFAULT 0,
            anio_registro INTEGER       NOT NULL,
            FOREIGN KEY (cod_hilado) REFERENCES alm_hilado(cod_hilado)
                ON UPDATE CASCADE ON DELETE RESTRICT
        );
        ''')
        self.cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_consumo_hilado_cod ON consumo_hilado(cod_hilado);")
 
        self.conn.commit()
 
    # ------------------------------------------------------------------
    # Utilidades internas
    # ------------------------------------------------------------------
    def _limpiar_codigos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza cod_hilado (str + strip) y descarta filas con código vacío/basura."""
        df = df.copy()
        df['cod_hilado'] = df['cod_hilado'].astype(str).str.strip()
 
        invalidos = df['cod_hilado'].isin(self.CODIGOS_INVALIDOS)
        if invalidos.any():
            print(f"Aviso: se descartaron {invalidos.sum()} filas con cod_hilado vacío o inválido.")
            df = df[~invalidos]
        return df
 
    def _asegurar_hilados(self, df: pd.DataFrame):
        """Inserta en alm_hilado los cod_hilado de df que aún no existan (evita error de FK)."""
        codigos = pd.DataFrame({'cod_hilado': df['cod_hilado'].dropna().unique()})
 
        existentes = pd.read_sql_query("SELECT cod_hilado FROM alm_hilado", self.conn)['cod_hilado']
        faltantes = codigos[~codigos['cod_hilado'].isin(existentes)].copy()
 
        if faltantes.empty:
            return
 
        faltantes['descripcion'] = 'SIN DESCRIPCION'
        faltantes.to_sql('alm_hilado', self.conn, if_exists='append', index=False)
        print(f"Aviso: se agregaron {len(faltantes)} hilados que no estaban en el catálogo: "
              f"{faltantes['cod_hilado'].tolist()}")
 
    # ------------------------------------------------------------------
    # Migraciones
    # ------------------------------------------------------------------
    def migrar_catalogo(self, df_almacen_unico: pd.DataFrame):
        """Carga solo los hilados nuevos, evitando errores de duplicados."""
        df = df_almacen_unico.rename(columns={
            "Cod Hilado": "cod_hilado",
            "Hilado": "descripcion",
        })
        df = self._limpiar_codigos(df)
        df['descripcion'] = df['descripcion'].fillna('SIN DESCRIPCION').astype(str)
 
        existentes = pd.read_sql_query("SELECT cod_hilado FROM alm_hilado", self.conn)['cod_hilado']
 
        df_nuevos = (
            df[~df['cod_hilado'].isin(existentes)]
            .drop_duplicates(subset=['cod_hilado'])
            [['cod_hilado', 'descripcion']]
        )
 
        if not df_nuevos.empty:
            df_nuevos.to_sql('alm_hilado', self.conn, if_exists='append', index=False)
            print(f"Se insertaron {len(df_nuevos)} nuevos hilados.")
        else:
            print("No se encontraron hilados nuevos en este archivo.")
 
    def migrar_movimientos(self, df_mov_filtrado: pd.DataFrame):
        """Prepara y carga los movimientos."""
        df_movimientos_sql = df_mov_filtrado.rename(columns={
            "OP Referencial": "op",
            "Cod Hilado": "cod_hilado",
            "Fecha": "fecha",
            "Tipo": "tipo_mov",
            "Cliente": "cliente",
            "Transaccion": "transaccion",
            "Aspecto": "aspecto",
            "Proveedor": "proveedor",
            "Num.Mov.": "num_mov",
            "Guia": "guia",
            "OC": "oc",
            "Factura": "factura",
            "Kgs": "kgs",
            "dolares": "dolares",
            "cu_usd": "cu_usd",
            "Soles": "soles",
            "cu_soles": "cu_soles"
        })
 
        # Normalizar códigos y garantizar que existan en el catálogo (FK)
        df_movimientos_sql = self._limpiar_codigos(df_movimientos_sql)
        self._asegurar_hilados(df_movimientos_sql)
 
        columnas_finales_mov = [
            "cod_hilado", "op", "tipo_mov", "fecha", "cliente", "aspecto", "categoria",
            "codigo", "proveedor", "num_mov", "transaccion", "guia", "oc", "factura",
            "kgs", "dolares", "cu_usd", "cu_usd_calc", "soles", "cu_soles", "anio_registro"
        ]
        df_movimientos_sql = df_movimientos_sql[columnas_finales_mov].copy()
        df_movimientos_sql['fecha'] = pd.to_datetime(df_movimientos_sql['fecha']).dt.strftime('%Y-%m-%d')
 
        df_movimientos_sql.to_sql('mov_hilado', self.conn, if_exists='append', index=False)
        print("¡Movimientos de hilados migrados con éxito!")
 
    def migrar_consumos(self, df_consumo: pd.DataFrame):
        """Prepara y carga los consumos."""
        df_consumo_sql = df_consumo.rename(columns={
            "OP Referencial": "op",
            "Cod Hilado": "cod_hilado",
            "Fecha": "fecha",
            "Tipo": "tipo_mov",
            "Cliente": "cliente",
            "Aspecto": "aspecto",
            "Proveedor": "proveedor",
            "Num.Mov.": "num_mov",
            "Transaccion": "transaccion",
            "Guia": "guia",
            "OC": "oc",
            "Factura": "factura",
            "Kgs": "kgs",
            "dolares": "dolares",
            "cu_usd": "cu_usd",
            "Soles": "soles",
            "cu_soles": "cu_soles"
        })
 
        # Normalizar códigos y garantizar que existan en el catálogo (FK)
        df_consumo_sql = self._limpiar_codigos(df_consumo_sql)
        self._asegurar_hilados(df_consumo_sql)
 
        columnas_consumo = [
            "cod_hilado", "op", "tipo_mov", "fecha", "cliente", "aspecto", "categoria",
            "codigo", "proveedor", "num_mov", "transaccion", "guia", "oc", "factura",
            "kgs", "dolares", "cu_usd", "cu_usd_calc", "soles", "cu_soles", "anio_registro"
        ]
        df_consumo_sql = df_consumo_sql[columnas_consumo].copy()
        df_consumo_sql['fecha'] = pd.to_datetime(df_consumo_sql['fecha']).dt.strftime('%Y-%m-%d')
 
        df_consumo_sql.to_sql('consumo_hilado', self.conn, if_exists='append', index=False)
        print("¡Movimientos de consumo de hilados migrados con éxito!")
 
    def __del__(self):
        """Cierra la conexión de forma segura al destruir el objeto."""
        try:
            self.conn.close()
        except Exception:
            pass