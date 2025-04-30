import os
from dotenv import load_dotenv

# Carga las variables del archivo .env
load_dotenv(dotenv_path=os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.env')))



# ===== RUTAS GENERALES =====
# Carpeta donde se guardan los datos crudos
DATA_RAW_DIR = os.getenv('DATA_RAW_DIR', 'data/raw')
# Carpeta para datos procesados
DATA_PROCESSED_DIR = os.getenv('DATA_PROCESSED_DIR', 'data/processed')
# Carpeta para archivos temporales
TMP_DIR = os.getenv('TMP_DIR', 'tmp')
# Nombre de archivo de datos unificados
DATA_UNITY = os.getenv('DATA_UNITY', 'combustibles_latam.csv')

# ===== ARGENTINA =====
# URL para automatizar la descarga
AR_DATASET_URL = os.getenv('AR_DATASET_URL')
# Nombre del archivo descargado
AR_DATASET_FILENAME = os.getenv('AR_DATASET_FILENAME', 'preciosArgentina.csv')
# Ruta completa al archivo crudo descargado
AR_DATASET_RAW_PATH = os.path.join(DATA_RAW_DIR, AR_DATASET_FILENAME)


# ===== URUGUAY =====
UR_DATASET_URL = os.getenv('UR_DATASET_URL')
UR_DATASET_FILENAME = os.getenv('UR_DATASET_FILENAME', 'preciosUruguay.csv')
UR_DATASET_RAW_PATH = os.path.join(DATA_RAW_DIR, UR_DATASET_FILENAME)


# ===== CHILE =====
CL_DATASET_URL = os.getenv('CL_DATASET_URL')
CL_DATASET_FILENAME = os.getenv('CL_DATASET_FILENAME', 'preciosChile.pdf')
CL_DATASET_RAW_PATH = os.path.join(DATA_RAW_DIR, CL_DATASET_FILENAME)

# ===== PERÚ =====
PE_DATASET_URL = os.getenv('PE_DATASET_URL')
PE_DATASET_FILENAME_RAW = os.getenv('PE_DATASET_FILENAME', 'preciosPeru.pdf')
PE_DATASET_FILENAME_PROCESSED = os.getenv('PE_DATASET_FILENAME', 'preciosPeru.csv')
PE_DATASET_RAW_PATH = os.path.join(DATA_RAW_DIR, PE_DATASET_FILENAME_RAW)

# ===== URL API =====
URL_API_OER = os.getenv('URL_API_OER')

# ===== API KEY ===== (cargada en "transformation")
API_KEY_OER = os.getenv('API_KEY_OER')


