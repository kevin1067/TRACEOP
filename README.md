# crear un .env en el proyecto:

TESTING_PATH="C:\\Users\\{tu nombre de usuario}\\Desktop\\TRACEOP\\datos_excel" #ruta local para pruebas de extraccion y migración de datos 

DB_PRODUCTION_PATH="C:\\Users\\{tu nombre de usuario}\\Desktop\\TRACEOP\\migracion" #esta ruta será la de cloud sql (gcp)

DB_TESTING_PATH="C:\\Users\\{tu nombre de usuario}\\Desktop\\TRACEOP\\migracion"

- Crea una carpeta "migracion" en la raiz del proyecto
- Crea una carpeta "datos_excel" en la raiz del proyecto, dentro de la carpeta crea una carpeta con el año 2026, en esa carpeta subes los datos de enero a junio de movimientos.
- Crea un entorno virtual y activalo
- Una vez activado, instala las librerias necesarias para iniciar el ETL:

```bash
pip install -r requirements.txt
```

- Una vez instalado ejecutar el script:

```bash
py main.py
```