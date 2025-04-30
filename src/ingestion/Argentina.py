import os, requests, sys, logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from config.config import AR_DATASET_URL, AR_DATASET_RAW_PATH


def extraer_datos_Argentina(url:str, destino:str):
    
    try:
        logging.info(f'Iniciando proceso de extracción de datos de Argentina desde: {url}')
        response = requests.get(url)
        response.raise_for_status()
        
        # Crear directorio si no existe
        os.makedirs(os.path.dirname(destino), exist_ok=True)
        
        # Guardar el archivo
        with open(destino, 'wb') as file:
            file.write(response.content)
        logging.info(f'Datos de Argentina guardados con éxito en: {destino}')
        
    except requests.exceptions.RequestException as e:
        logging.error(f'No se pudo descargar los datos de Argentina: {e}')
        raise

if __name__ == '__main__':
    extraer_datos_Argentina(AR_DATASET_URL, AR_DATASET_RAW_PATH)
    
