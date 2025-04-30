import os, sys
import pandas as pd

# Agrega la raíz del proyecto al sys.path para importar módulos correctamente
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Importar configuración de logs
from src.utils.log_config import logging
# Importar el esquema de validación de Pandera
from src.utils.schema import schema_argentina, schema_uruguay, schema_peru

# Importar funciones de procesamiento individuales
from src.transformation.Argentina import procesar_datos_Argentina
from src.transformation.Uruguay import procesar_datos_Uruguay
from src.transformation.Peru import procesar_datos_Peru

# Importar variables desde config
from config.config import (AR_DATASET_RAW_PATH, 
                           UR_DATASET_RAW_PATH,
                           PE_DATASET_RAW_PATH,
                           DATA_PROCESSED_DIR,
                           DATA_UNITY)



def ejecutar_procesamiento():
    
    logging.info('Iniciando procesamiento de datos para Latam...')
    
    # Diccionario para guardar errores de validación de datos
    errores_validacion = {}
    
    try:
        df_argentina = procesar_datos_Argentina(AR_DATASET_RAW_PATH)
        schema_argentina.validate(df_argentina)
    except Exception as e:
        logging.error(f'Error en la validación de datos de Argentina.')
        errores_validacion['Argentina'] = str(e)
    
    try:
        df_uruguay = procesar_datos_Uruguay(UR_DATASET_RAW_PATH)    
        schema_uruguay.validate(df_uruguay)
    except Exception as e:
        logging.error('Error en la validación de datos de Uruguay.')
        errores_validacion['Uruguay'] = str(e)
        
    try:    
        df_peru = procesar_datos_Peru(PE_DATASET_RAW_PATH)    
        schema_peru.validate(df_peru)
    except Exception as e:
        logging.error('Error en la validación de datos de Perú.')
        errores_validacion['Perú'] = str(e)
    
    # Si hubo errores levantar excepción general
    if errores_validacion:
        mensaje_error = '\n'.join([f'{pais}:{error}' for pais, error in errores_validacion.items()])
        raise Exception(f'Errores de validación encontrados:\n{mensaje_error}')
    
    # Se unifican los DFs para ser guardados en un solo archivo csv
    ruta_salida = os.path.join(DATA_PROCESSED_DIR, DATA_UNITY)
    df_latam = pd.concat([df_argentina, df_uruguay, df_peru], ignore_index=True)
    df_latam.to_csv(ruta_salida, index=False)
    
    logging.info(f'Archivo de datos unificados guardado con éxito en: {ruta_salida}')

if __name__ == '__main__':
    ejecutar_procesamiento()