import pandas as pd
import os, pdfplumber

from src.utils.cotizaciones import obtener_cotizaciones
from src.utils.log_config import logging
from config.config import DATA_PROCESSED_DIR, PE_DATASET_RAW_PATH, PE_DATASET_FILENAME_PROCESSED

# Elimina la visualizacipon de los mensajes "warning"
logging.getLogger("pdfminer").setLevel(logging.ERROR)

def procesar_datos_Peru(
    data_cruda,
    columnas_orden=None):
    
    try:
        logging.info('=== Iniciando procesamiento de datos para Perú. ===')
        
        # Defaults si no se pasaron argumentos
        if columnas_orden is None:
            columnas_orden = ['pais', 'fecha', 'localidad', 'establecimiento', 'producto', 'precio_litro', 'precio_usd_litro']
        
        
        # 2. Coordenadas de cada tablas en cada página del pdf
        bboxes_por_tabla = [
            (100, 180, 580, 532), # Tabla 1 - Página 3
            (120, 600, 595, 800), # Tabla 2 - Página 3
            (120, 30, 595, 310),  # Tabla 2.2 - Página 4 (No hay tabla 3)
            (120, 460, 595, 565), # Tabla 4 - Página 4
            (120, 650, 595, 740), # Tabla 5 - Página 4
            (120, 290, 595, 685), # Tabla 7 - Página 5
            (120, 220, 595, 610), # Tabla 6 - Página 6
            (120, 700, 595, 800), # Tabla 7 - Página 6
            (120, 50, 595, 345), # Tabla 7.1 - Página 7
            (120, 435, 595, 840), # Tabla 8 - Página 7
            (120, 55, 595, 165), # Tabla 8.1 - Página 8
            (120, 255, 595, 485) # Tabla 9 - Página 8
        ]
        paginas = [2, 2, 3, 3, 3, 4, 5, 5, 6, 6, 7, 7]
        tablas_finales = []
        
        # 3. Extraer y extructurar información tabular desde varias zonas de un pdf
        with pdfplumber.open(data_cruda) as file:
            for i, bbox in enumerate(bboxes_por_tabla):
                page = file.pages[paginas[i]]
                cropped = page.within_bbox(bbox)
                table = cropped.extract_table()
                
                if table and len(table) > 2:
                    header = ['Departamento', 'Mín. de Venta', 'Promedio de Venta', 'Máx. de Venta']
                    
                    data = []
                    for row in table[2:]:
                        try:
                            data.append([row[1], row[4], row[7], row[10]])
                        except IndexError:
                            continue
                    
                    df = pd.DataFrame(data, columns=header)
                    tablas_finales.append(df)
        
        # 4. Se agrega columna "Combustible" para identificar cada registro
        combustibles = [
            'Gasohol Regular',
            'Gasohol Premium',
            'Gasohol Premium',
            'Gasolina Regular',
            'Gasolina Premium',
            'Diesel B5 S50 UV',
            'GLP Vehícular',
            'GLP Envasado',
            'GLP Envasado',
            'GLP Envasado', 
            'GLP Envasado'
        ]

        # 5. Se agrega la columna "Establecimiento de Ventas"
        establecimientos_ventas = [
            'Grifos y EE.SS',
            'Gasocentros, Grifos y EE.SS',
            'Gasocentros, Grifos y EE.SS',
            'Grifos y EE.SS',
            'Grifos y EE.SS',
            'Grifos y EE.SS',
            'Gasocentros, Grifos y EE.SS',
            'Grifos y EE.SS',
            'Locales de Venta',
            'Locales de Venta',
            'Plantas Envasadoras' 
            ]
        
        # 6. Agrega columnas 
        for i, (df, combustibles, establecimientos_ventas) in enumerate(zip(tablas_finales, combustibles, establecimientos_ventas)):
            df['Combustible'] = combustibles
            df['Establecimientos de Venta'] = establecimientos_ventas
            tablas_finales[i] = df

        # 7. Concatena todas las tablas en un solo DF
        df = pd.concat(tablas_finales, ignore_index=True)
        
        # 8. Se renombran las columnas
        df = df.rename(columns={
            'Departamento' : 'localidad',
            'Promedio de Venta' : 'precio_litro',
            'Combustible' : 'producto',
            'Establecimientos de Venta' : 'establecimiento'
        })    
        
        # 9. Se crean columnas
        df['pais'] = 'Perú'
        df['fecha'] = '2025-10-04'
        
        # 10. Convertir tipos de datos
        df['precio_litro'] = pd.to_numeric(df['precio_litro'], errors='coerce')
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
        
        # 11. Reemplazar nulo con mediana en "precio_litro"
        mediana_precio = df['precio_litro'].median()
        df['precio_litro'] = df['precio_litro'].fillna(mediana_precio)

        # 12. Obtener cotización del dolar para "PEN"
        tasa = obtener_cotizaciones('PEN')

        # 13. Crear columna "precio_usd_litro" y calcular conversión
        df['precio_usd_litro'] = (df['precio_litro'] / tasa).round(2)

        # 14. Formato estandarizado
        df['fecha'] = df['fecha'].dt.strftime('%Y-%m-%d')
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
        
        # 15. Eliminar duplicados
        registros_originales = len(df)
        df = df.drop_duplicates()
        registros_nuevos = len(df) 
        logging.info(f'Se eliminaron {registros_originales - registros_nuevos} registros duplicados.')
        
        # 16. Se ordenan columnas 
        df = df[columnas_orden] 
        
        # 17. Cargar datos procesados
        ruta_salida = os.path.join(DATA_PROCESSED_DIR, PE_DATASET_FILENAME_PROCESSED)
        df.to_csv(ruta_salida, index=False)
        logging.info(f'Archivo guardado con éxito en: {ruta_salida}')
        
        logging.info('=== Procesamiento de Perú realizado con éxito. ===') 
        return df
    
    except Exception as e:
        logging.error(f'Error durante el procesamiento de datos para Perú: {e}')
        raise
    
if __name__ == '__main__':
    procesar_datos_Peru(PE_DATASET_RAW_PATH)