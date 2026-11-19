import pandas as pd
from pathlib import Path


class TransformTejido:
    def __init__(self, df_alm_tejeduria: pd.DataFrame,dict_codigos:dict):
        self.df_alm_tejeduria = df_alm_tejeduria
        self.dict_codigos = dict_codigos

    def getAlmTejeduria(self, col_alm_tejeduria: list[str]) -> pd.DataFrame:
        df_filtrado = self.df_alm_tejeduria[col_alm_tejeduria].copy()
        
        # 1. Limpieza robusta de la LLAVE PRIMARIA (Partida) y Cod_Tela
        df_filtrado['Partida'] = df_filtrado['Partida'].fillna('').astype(str).str.strip()
        df_filtrado['Cod_Tela'] = df_filtrado['Cod_Tela'].fillna('').astype(str).str.strip()
        
        # 2. Eliminar filas donde la Partida esté vacía (es obligatorio para la Primary Key de SQL)
        df_filtrado = df_filtrado[df_filtrado['Partida'] != '']
        
        # 3. CORRECCIÓN CRÍTICA: La unicidad debe ser por 'Partida', no por 'Cod_Tela'
        df_almacen_unico = df_filtrado.drop_duplicates(subset=['Partida'], keep='first')
        
        return df_almacen_unico
    
    def getMovTejeduria(self, year: int, aspecto_mov_tejeduria: list[str],cols_mov_tejeduria: list[str]) -> pd.DataFrame:
        df_filtrado = self.df_alm_tejeduria[cols_mov_tejeduria].copy()
        df_filtrado = df_filtrado[df_filtrado['OP Referencial'].fillna('').str.strip() != '']
        df_filtrado['OP Referencial'] = (df_filtrado['OP Referencial']
                                        .astype(str).str.replace(r'\.0$', '', regex=True))
        df_filtrado['Partida'] = df_filtrado['Partida'].fillna('').astype(str).str.strip()
        df_filtrado['Cod_Tela'] = df_filtrado['Cod_Tela'].fillna('').astype(str).str.strip()
        df_filtrado['Aspecto'] = df_filtrado['Aspecto'].fillna('').astype(str).str.strip()
        df_filtrado['Transaccion'] = df_filtrado['Transaccion'].fillna('').astype(str).str.strip()

        df_limpio = df_filtrado[df_filtrado["Aspecto"].isin(aspecto_mov_tejeduria)].copy()

        # --- Eliminar duplicados SOLO en transacciones TCM ---
        df_limpio = self._deduplicar_tcm(df_limpio)

        df_cod = self.addCodigos(df_limpio)
        df_tela_principal = self.transformCatTelaPrincipal(df_cod)
        df_final = self.actualizarPU(df_tela_principal)
        df_final['anio_registro'] = year
        return df_final
    

    def _deduplicar_tcm(self, df: pd.DataFrame) -> pd.DataFrame:
        """Elimina filas duplicadas únicamente en transacciones TCM; el resto no se toca."""
        mask_tcm = df['Transaccion'].str.startswith('TCM')

        claves = ['OP Referencial', 'Cod_Tela', 'Partida', 'Aspecto', 'Transaccion',
                'Fecha', 'Guia', 'Kgs', 'dolares']
        claves = [c for c in claves if c in df.columns]

        df_tcm = df[mask_tcm].drop_duplicates(subset=claves, keep='first')
        df_resto = df[~mask_tcm]

        print(f"TCM: {mask_tcm.sum() - len(df_tcm)} duplicados eliminados.")

        # sort_index restaura el orden original (necesario para el ffill de actualizarPU)
        return pd.concat([df_resto, df_tcm]).sort_index()

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

    def transformCatTelaPrincipal(self, df_modificado: pd.DataFrame) -> pd.DataFrame:

        df_modificado.loc[
            (df_modificado['categoria'] == 'Servicio de tejeduría') &
            (df_modificado['Cod_Tela'].str.startswith('C', na=False)),
            'categoria'
        ] = 'Servicio de Tejeduría - Tela principal'

        return df_modificado
        
    def actualizarPU(self, df_alm_tejeduria: pd.DataFrame) -> pd.DataFrame:
        claves = ['OP Referencial', 'Partida', 'Cod_Tela', 'Medida']

        # Normalizar tipos de las claves para evitar mismatches en el cruce
        df_alm_tejeduria = df_alm_tejeduria.copy()
        for c in claves:
            df_alm_tejeduria[c] = df_alm_tejeduria[c].astype(str).str.strip()

        # 1. Filas JS8 (despacho a tintorería) - SIN sumar, fila a fila.
        # Como 'Medida' ya forma parte de las claves, cada combinación
        # debería identificar un único despacho JS8.
        mask_js8 = df_alm_tejeduria['Transaccion'].fillna('').astype(str).str.contains('JS8', na=False)
        js8_rows = df_alm_tejeduria.loc[mask_js8, claves + ['Kgs']].copy()
        js8_rows['Kgs'] = js8_rows['Kgs'].abs()

        # Aviso si hubiera más de un despacho JS8 con las mismas 4 claves
        # (no debería ocurrir si Medida distingue cada despacho)
        #dup_js8 = js8_rows.duplicated(subset=claves, keep=False)
        #if dup_js8.any():
        #    print("ADVERTENCIA: hay despachos JS8 duplicados para las mismas claves "
        #        "(OP, Partida, Cod_Tela, Medida). Revisar estas filas:")
        #    print(js8_rows.loc[dup_js8])

        js8_rows = js8_rows.rename(columns={'Kgs': 'kg_enviado'})
        js8_rows = js8_rows.drop_duplicates(subset=claves, keep='last')

        # 2. Filas T21 (ingreso a tintorería)
        mask_t21 = df_alm_tejeduria['Transaccion'].fillna('').astype(str).str.contains('T21', na=False)

        # 3. Cruce fila a fila (sin agregación) usando las 4 claves, incluyendo Medida
        df_t21 = df_alm_tejeduria.loc[mask_t21, claves + ['dolares', 'cu_usd']].merge(
            js8_rows, on=claves, how='left'
        )
        df_t21.index = df_alm_tejeduria.loc[mask_t21].index  # preservar índice original

        # 4. Evitar división por cero / NaN
        kg_seguro = df_t21['kg_enviado'].replace(0, pd.NA)

        # 5. Precio unitario sobre kilos enviados (JS8), fila a fila
        cu_calc = (df_t21['dolares'] / kg_seguro).fillna(df_t21['cu_usd'])

        # 6. Volcar resultado
        df_alm_tejeduria['cu_usd_calc'] = df_alm_tejeduria['cu_usd']
        df_alm_tejeduria.loc[mask_t21, 'cu_usd_calc'] = cu_calc
        df_alm_tejeduria['cu_usd_calc'] = df_alm_tejeduria['cu_usd_calc'].round(2)

        return df_alm_tejeduria