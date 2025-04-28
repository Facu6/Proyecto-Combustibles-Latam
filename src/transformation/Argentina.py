import pandas as pd 
import os

from src.utils.log_config import logging
from src.utils.cotizaciones import obtener_cotizaciones
from config.config import DATA_PROCESSED_DIR, AR_DATASET_RAW_PATH, AR_DATASET_FILENAME

def procesar_datos_Argentina(
    data_cruda, 
    columnas_renombrar=None,
    columnas_finales=None ,
    columnas_orden=None):
    
    try:
        logging.info('=== Iniciando procesamiento de datos para Argentina. ===')
        
        # Defaults si no se pasaron argumentos
        if columnas_renombrar is None:
            columnas_renombrar = {
                'fecha_vigencia' : 'fecha',
                'precio' : 'precio_litro',
                'empresabandera' : 'establecimiento'
            }
        if columnas_finales is None:
            columnas_finales = ['pais', 'fecha', 'producto', 'precio_litro', 'localidad', 'establecimiento']
        
        if columnas_orden is None:
            columnas_orden = ['pais', 'fecha', 'localidad', 'establecimiento', 'producto', 'precio_litro', 'precio_usd_litro']
            
        
        # 1. Leer archivo
        df = pd.read_csv(data_cruda)
        logging.info(f'Archivo leído correctamente desde: {data_cruda}')
        
        # 2. Renombrar columnas
        df.rename(columns = columnas_renombrar, inplace = True)
        
        # 3. Agregar columna "pais"
        df['pais'] = 'Argentina'
        
        # 4. Se seleccionan las columnas finales a trabajar
        df = df[columnas_finales]
        
        # 5. Convertir tipos datos
        df['precio_litro'] = pd.to_numeric(df['precio_litro'], errors='coerce') 
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')        
        
        # 6. Obtener cotización del dolar para "ARS"
        tasa = obtener_cotizaciones('ARS')
        
        # 6.1 Crear columna "precio_usd_litro" y calcular conversión
        df['precio_usd_litro'] = (df['precio_litro'] / tasa).round(2)
        
        # 7. Formato estandarizado
        df['fecha'] = df['fecha'].dt.strftime('%Y-%m-%d')
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
        
        # 8. Eliminar duplicados
        registros_originales = len(df)
        df = df.drop_duplicates()
        registros_nuevos = len(df)
        logging.info(f'Se eliminaron {registros_originales - registros_nuevos} registros duplicados.')
        
        # 9. Ordenar columnas
        df = df[columnas_orden]
        
        # 10. Cargar datos procesados
        ruta_salida = os.path.join(DATA_PROCESSED_DIR, AR_DATASET_FILENAME)
        df.to_csv(ruta_salida, index=False)
        logging.info(f'Archivo guardado con éxito en: {ruta_salida}')
        
        logging.info('=== Procesamiento de Argentina realizado con éxito. ===')
        return df
    
    except Exception as e:
        logging.error(f'Error durante el procesamiento de datos para Argentina: {e}')
        raise

if __name__ == '__main__':
    procesar_datos_Argentina(AR_DATASET_RAW_PATH)