import sys, os

# Agrega la raíz del proyecto al sys.path para importar módulos correctamente
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Importar configuración de logs
from src.utils.log_config import logging

# Importar funciones de extracción individuales
from src.ingestion.Argentina import extraer_datos_Argentina
from src.ingestion.Uruguay import extraer_datos_Uruguay
from src.ingestion.Peru import extraer_datos_Peru

# Importar rutas y URLs desde el config
from config.config import(
    AR_DATASET_URL, AR_DATASET_RAW_PATH,
    UR_DATASET_URL, UR_DATASET_RAW_PATH,
    PE_DATASET_URL, PE_DATASET_RAW_PATH
)

def ejecutar_extracciones():
    
    logging.info('===== INICIO DE EXTRACCIÓN DE DATOS =====')
    
    extraer_datos_Argentina(AR_DATASET_URL, AR_DATASET_RAW_PATH)
    extraer_datos_Uruguay(UR_DATASET_URL, UR_DATASET_RAW_PATH)
    extraer_datos_Peru(PE_DATASET_URL, PE_DATASET_RAW_PATH)
    
    logging.info('===== FIN DE EXTRACCIÓN DE DATOS =====')
    
if __name__ == '__main__':
    ejecutar_extracciones()
