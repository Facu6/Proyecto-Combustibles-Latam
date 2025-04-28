import os, sys, requests, logging

# Agrega la raíz del proyecto al sys.path para buscar "config/config.py"
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from config.config import PE_DATASET_URL, PE_DATASET_RAW_PATH

def extraer_datos_Peru(url:str, destino:str):
    
    try:
        logging.info(f'Iniciando proceso de extracción de datos de Perú desde: {url}')
        response = requests.get(url)
        response.raise_for_status()
        
        # Crear directorio si no existe
        os.makedirs(os.path.dirname(destino), exist_ok=True)
        
        # Guardar el archivo
        with open(destino, 'wb') as file:
            file.write(response.content)
        logging.info(f'Datos de Perú guardados con éxito en: {destino}')
        
    except requests.exceptions.RequestException as e:
        logging.error(f'No se pudo descargar los datos de Perú: {e}')
        raise


if __name__ == '__main__':
    extraer_datos_Peru(PE_DATASET_URL, PE_DATASET_RAW_PATH)