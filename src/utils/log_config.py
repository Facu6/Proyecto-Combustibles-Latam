import logging
import os

# Crear carpeta "logs" si no existe 
LOG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs'))
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE_PATH = os.path.join(LOG_DIR, 'ingestion.log')

# Configuración general del loggin
logging.basicConfig(
    level = logging.INFO,
    format = '[%(asctime)s] [%(levelname)s] %(message)s',
    handlers = [
        logging.FileHandler(LOG_FILE_PATH, encoding = 'utf-8'),
        logging.StreamHandler()
    ]
)