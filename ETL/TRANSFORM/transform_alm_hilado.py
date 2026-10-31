import pandas as pd
from pathlib import Path
import numpy as np


class TransformHilado:

    def __init__(self,df_alm_hilado:pd.DataFrame,dict_codigos:dict):
        self.df_alm_hilado = df_alm_hilado
        self.dict_codigos = dict_codigos


    def getAlmHilado(self, col_alm: list[str]) -> pd.DataFrame:
        #df = pd.read_excel(self.data_source, skiprows=3)

        # 1. Filtrar el DataFrame para quedarte solo con las columnas deseadas

        df_filtrado = self.df_alm_hilado[col_alm].copy()
            
        # 2. Limpieza (Formatear a minúsculas 'Lote' y 'Proveedor' como indican tus comentarios)
        #df_filtrado['Lote'] = df_filtrado['Lote'].astype(str)
        df_filtrado['Cod Hilado']=df_filtrado['Cod Hilado'].str.strip()

        #elimiar valores de cod hilado que estén vacios 
            
        df_filtrado = df_filtrado[df_filtrado['Cod Hilado'].str.strip() != '']
        # 3. Filtrar para que "Cod Hilado" sea ÚNICO
        # keep='first' conserva la primera aparición de cada código
        df_almacen_unico = df_filtrado.drop_duplicates(subset=['Cod Hilado'], keep='first')
            
        return df_almacen_unico


    def getMovHilado(self, year:int, aspecto_movimientos: list[str], col_mov: list[str]) -> pd.DataFrame:

    
        df_filtrado = self.df_alm_hilado[col_mov].copy()
        df_filtrado = df_filtrado[df_filtrado['OP Referencial'].fillna('').str.strip() != '']

        df_filtrado['OP Referencial'] = df_filtrado['OP Referencial'].astype(str)

        df_filtrado['Cod Hilado'] = df_filtrado['Cod Hilado'].str.strip()
        df_filtrado['Transaccion'] = df_filtrado['Transaccion'].str.strip()
        df_filtrado['Aspecto'] = df_filtrado['Aspecto'].str.strip()
        df_filtrado['OP Referencial'] = df_filtrado['OP Referencial'].str.replace(r'\.0$', '', regex=True)
            
        # 1. FILTRAR PRIMERO por Aspecto (define si es movimiento de tejeduría)
        df_filtrado_movs = df_filtrado[df_filtrado["Aspecto"].isin(aspecto_movimientos)].copy()

        df_y18_modificado = self.transformMovY18(df_filtrado_movs)
        df_x18_modificado = self.transformMovX18(df_y18_modificado)
        df_filtrado_movs_unico = df_x18_modificado.drop_duplicates(
            subset=["OP Referencial", "Num.Mov.", "Kgs"], 
            keep='first'
        ).copy()

        df_ordenado = df_filtrado_movs_unico.sort_values(
            by=['OP Referencial', 'Cod Hilado', 'Fecha', 'Num.Mov.']
        ).reset_index(drop=True)
        #df_filtrado_movs_unico = df_filtrado_movs.drop_duplicates(subset=["llave"], keep='first')
            
        df_categoria_cod = self.addCodigos(df_ordenado)
        df_categoria_cod = self.actualizarPU(df_categoria_cod)
        df_categoria_cod['anio_registro'] = year

            
            
        return df_categoria_cod
    
    def getMovConsumoHilado(self, year:int, aspecto_consumo: list[str], col_mov_consumo: list[str]) -> pd.DataFrame:
        df = self.df_alm_hilado[col_mov_consumo].copy()
        df = df[df['OP Referencial'].fillna('').str.strip() != '']
        df['OP Referencial'] = df['OP Referencial'].astype(str).str.replace(r'\.0$', '', regex=True)
        df["Aspecto"] = df["Aspecto"].str.strip()
        df["Cod Hilado"] = df["Cod Hilado"].str.strip() 
            
        df_filtrado = df[df["Aspecto"].isin(aspecto_consumo)]
        #df_filtrado['OP Referencial'] = df_filtrado['OP Referencial'].astype(str)
        #df_filtrado['OP Referencial'] = df_filtrado['OP Referencial'].str.replace(r'\.0$', '', regex=True)
        df_filtrado_unico = df_filtrado[df_filtrado["Kgs"] != 0].copy()

            
        # --- NUEVA LÓGICA: Actualización de Transacción según Aspecto ---
        # NOTA: Cambia "Transacción" por el nombre exacto de la columna en tu DataFrame
        # si lo manejas distinto en 'col_mov_consumo' (ej: "Transaccion" sin tilde).
        col_trans = "Transaccion" 
            
        # Máscaras booleanas para ubicar las filas correctas
        mask_consumido = df_filtrado_unico["Aspecto"] == "Hilado Consumido"
        mask_merma = df_filtrado_unico["Aspecto"] == "Hilado Merma"
            
        # Asignación directa usando .loc para las condiciones
        df_filtrado_unico.loc[mask_consumido, col_trans] = "JE4C-Ingresos Produccion Servicios Sin Pesado Previo"
        df_filtrado_unico.loc[mask_merma, col_trans] = "JE4M-Ingresos Produccion Servicios Sin Pesado Previo"
        # ----------------------------------------------------------------
        df_categoria_cod = self.addCodigos(df_filtrado_unico)
        df_categoria_cod['cu_usd_calc'] = df_categoria_cod['cu_usd']
        df_categoria_cod['anio_registro'] = year
        return df_categoria_cod

    def addCodigos(self, df: pd.DataFrame) -> pd.DataFrame:
    
            df_modificado = df.copy()
            df_modificado['codigo'] = (
                df_modificado['Transaccion']
                .str.split('-')
                .str[0]
            )
    
            # Buscar códigos no registrados
            codigos_nuevos = set(df_modificado['codigo'].unique()) - set(self.dict_codigos.keys())
    
            if codigos_nuevos:
                raise ValueError(
                    f"Se encontraron códigos no configurados: {sorted(codigos_nuevos)}"
                )
    
            df_modificado['categoria'] = (
                df_modificado['codigo']
                .map(self.dict_codigos)
            )
    
            return df_modificado
    
    def transformMovY18(self, df: pd.DataFrame) -> pd.DataFrame:
        df['OC'] = df['OC'].replace('', pd.NA)

        llaves = ['Num.Mov.', 'OP Referencial', 'Guia']
        mask_y18 = df['Transaccion'].str.contains('Y18', na=False)

        # Propagar el OC dentro del bloque
        df.loc[mask_y18, 'OC'] = (
            df.loc[mask_y18]
            .groupby(llaves)['OC']
            .transform(lambda s: s.ffill().bfill())
        )

        # Marcar cuál fila está valorizada (Soles o dolares != 0)
        df['_valorizado'] = (df['Soles'].fillna(0) != 0) | (df['dolares'].fillna(0) != 0)

        # Ordenar para que la fila valorizada quede primero dentro de cada bloque
        df = df.sort_values(by=llaves + ['_valorizado'], ascending=[True, True, True, False])

        df = df.drop(columns='_valorizado')
        # Regla de negocio: todo Y18 es Tipo 'E' (Entrada/Devolución)
        df.loc[mask_y18, 'Tipo'] = 'E'
        return df

    def transformMovX18(self, df: pd.DataFrame) -> pd.DataFrame:
        mask_x18 = df['Transaccion'].str.contains('X18', na=False)

        cols_a_negativizar = ['Kgs', 'dolares', 'Soles']

        df.loc[mask_x18, cols_a_negativizar] = df.loc[mask_x18, cols_a_negativizar] * -1

        return df
    
    def actualizarPU(self, df_alm_hilado: pd.DataFrame) -> pd.DataFrame:
        # 1. Kgs base de las salidas X18 (float, con NaN)
        mask_x18 = df_alm_hilado['codigo'].astype(str).str.contains('X18', na=False)
        df_alm_hilado['kg_base_x18'] = np.nan
        df_alm_hilado.loc[mask_x18, 'kg_base_x18'] = (
            pd.to_numeric(df_alm_hilado.loc[mask_x18, 'Kgs'], errors='coerce').abs()
        )

        # 2. Arrastrar hacia abajo por OP y Cod Hilado
        df_alm_hilado['kg_base_x18'] = (
            df_alm_hilado.groupby(['OP Referencial', 'Cod Hilado'])['kg_base_x18'].ffill()
        ).astype('float64')

        # 3. Ingresos X34 y L34
        mask_objetivo = df_alm_hilado['codigo'].astype(str).str.contains('X34|L34', na=False)

        # 4. Columna calculada SIEMPRE como float64
        df_alm_hilado['cu_usd_calc'] = pd.to_numeric(
            df_alm_hilado['cu_usd'], errors='coerce'
        ).astype('float64')

        # 5. Evitar división por cero
        kg_seguro = df_alm_hilado['kg_base_x18'].replace(0, np.nan)

        # 6. Nuevo costo unitario
        dolares = pd.to_numeric(df_alm_hilado['dolares'], errors='coerce').astype('float64')
        nuevo_cu = (dolares / kg_seguro).where(mask_objetivo)

        # Solo reemplaza donde hay objetivo y el cálculo es válido
        usar_nuevo = mask_objetivo & nuevo_cu.notna()
        df_alm_hilado.loc[usar_nuevo, 'cu_usd_calc'] = nuevo_cu[usar_nuevo]

        # 7. Limpiar
        return df_alm_hilado.drop(columns=['kg_base_x18'])