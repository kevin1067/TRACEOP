import pandas as pd

class TransformResumen:

    def __init__(self,df_consolidado :pd.DataFrame,dict_codigos:dict):
        self.df_consolidado = df_consolidado
        self.dict_codigos = dict_codigos

    def getResumen(self) -> pd.DataFrame:
        
        df = self.df_consolidado.copy()
        #df_transformed = self.addCodigos(df)
        df_resumen = self.getTotal(df)

        return df_resumen
    
    

    def addCodigos(self,df:pd.DataFrame) -> pd.DataFrame:
        
        df_modificado = df.copy()
        df_modificado['codigo'] = df_modificado['transaccion'].str.split('-').str[0]
        df_modificado['categoria'] = df_modificado['codigo'].map(self.dict_codigos)

        return df_modificado

    def getTotal(self,df:pd.DataFrame):
        df_transformed = df.copy()
        df_filtered = df_transformed[(df_transformed["kgs"] != 0) & (df_transformed["dolares"] != 0)]
        df_resumen = df_filtered.groupby(['op','categoria']).agg(
        kg_netos=('kgs', 'sum'),
        valorizado_total=('dolares', 'sum')
        ).reset_index()

        df_resumen["kg_netos"] = df_resumen["kg_netos"].round(decimals=2)
        df_resumen["valorizado_total"] = df_resumen["valorizado_total"].round(decimals=2)
        
        # Cálculo del Precio Unitario Ponderado
        # Se utiliza una función lambda para mitigar errores de división por cero
        df_resumen['precio_unitario_ponderado'] = df_resumen.apply(
            lambda row: round(row['valorizado_total'] / row['kg_netos'], 2) 
            if row['kg_netos'] != 0 else 0,
            axis=1
        )

        return df_resumen
