import requests, os, sys, logging
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO

# Agrega la raíz del proyecto al sys.path para buscar "config/config.py"
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from config.config import UR_DATASET_URL, UR_DATASET_RAW_PATH

def extraer_datos_Uruguay(url:str, destino:str):
    
    try:
        logging.info(f'Iniciando proceso de extracción de datos de Uruguay desde: {url}')
        response = requests.get(url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Encontrar tabla con clase "texto" y estilo exacto
        tabla = soup.find('table', class_='texto', style='width: 101.595%;')
        
        if tabla is None:
            raise ValueError('No se encontró la tabla con los atributos especificados en el HTML.')
        
        # Convertir a DataFrame
        df = pd.read_html(StringIO(tabla.prettify()))[0]
        
        # Guardar
        os.makedirs(os.path.dirname(destino), exist_ok=True) 
        df.to_csv(destino, index=False)
        logging.info(f'Datos de Uruguay guardados con éxito en: {destino}')
    
    except Exception as e:
        logging.error(f'No se pudo descargar los datos de Uruguay: {e}')
        
        
if __name__ == '__main__':
    extraer_datos_Uruguay(UR_DATASET_URL, UR_DATASET_RAW_PATH)