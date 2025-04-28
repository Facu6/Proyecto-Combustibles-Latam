import pandas as pd
import os

from src.utils.log_config import logging
from src.utils.cotizaciones import obtener_cotizaciones
from config.config import DATA_PROCESSED_DIR, UR_DATASET_RAW_PATH, UR_DATASET_FILENAME


def procesar_datos_Uruguay(
    data_cruda,
    columnas_orden=None):
    
    try:
        logging.info('=== Iniciando procesamiento de datos para Uruguay. ===')
        
        # Defaults si no se pasaron argumentos
        if columnas_orden is None:
            columnas_orden = ['pais', 'fecha', 'localidad', 'establecimiento', 'producto', 'precio_litro', 'precio_usd_litro']
        
        # 1. Leer archivo
        df = pd.read_csv(data_cruda)
        
        # 2. Asignar los valores de la primera fila como nombres de columnas
        df.columns = df.iloc[0]
        
        # 3. Eliminar la primer fila del dataframe (ahora es encabezado)
        df = df[1:].reset_index(drop=True)
        
        # 4. Se aplica "melt" para pasar el df de formato ancho a largo
        df = df.melt(id_vars='Fecha', var_name='producto', value_name='precio_litro')
        df.rename(columns={'Fecha' : 'fecha'}, inplace=True)

        # 5. Se crean columnas
        df['pais'] = 'Uruguay'
        df['localidad'] = 'Nacional' # Se detalla "Nacional" ya que los precios informados por ANCAP suelen ser uniformes y aplicables a todo el país.
        df['establecimiento'] = 'Sin Dato'

        # 6. Se seleccionan las columnas del df final de Uruguay
        df = df[['pais', 'fecha', 'producto', 'precio_litro', 'localidad', 'establecimiento']]
        
        
        # 7. Convertir tipos de datos
        df['precio_litro'] = pd.to_numeric(df['precio_litro'], errors='coerce').astype(float) 
        df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True, errors='coerce') 

        # 8. Obtener cotización del dolar para "UYU"
        tasa = obtener_cotizaciones('UYU')
        
        # 8.1 Crear columna "precio_usd_litro" y calcular conversión
        df['precio_usd_litro'] = (df['precio_litro'] / tasa).round(2)

        # 9. Formato estandarizado
        df['fecha'] = df['fecha'].dt.strftime('%Y-%m-%d')
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
        
        # 10. Eliminar duplicados
        registros_originales = len(df)
        df = df.drop_duplicates()
        registros_nuevos = len(df)
        logging.info(f'Se eliminaron {registros_originales - registros_nuevos} registros duplicados.')
        
        # 11. Ordenar columnas
        df = df[columnas_orden]
        
        # 12. Cargar datos procesados
        ruta_salida = os.path.join(DATA_PROCESSED_DIR, UR_DATASET_FILENAME)
        df.to_csv(ruta_salida, index=False)
        logging.info(f'Archivo guardado con éxito en: {ruta_salida}')
        
        logging.info('=== Procesamiento de Uruguay realizado con éxito. ===')
        return df
        
    except Exception as e:
        logging.error(f'Error durante el procesamiento de datos para Uruguay: {e}')
        raise
    
if __name__ == '__main__':
    procesar_datos_Uruguay(UR_DATASET_RAW_PATH)