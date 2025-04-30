from google.cloud import storage, bigquery
from google.api_core.exceptions import Conflict
import os
# Importar configuración de logs
from src.utils.log_config import logging
# Importar schema para tablas de BQ
from src.utils.schema import schema_bigquery

# Importar variables de config
from config.config import(
    PROJECT_ID,
    BQ_DATASET_LATAM,
    BUCKET_NAME_LATAM,
    BQ_TABLE_UNITY,
    DATA_PROCESSED_DIR,
    DATA_UNITY_PROCESSED
)

# Importar funciones de carga individual
from src.load.Argentina import ejecutar_carga_argentina
from src.load.Uruguay import ejecutar_carga_uruguay
from src.load.Peru import ejecutar_carga_peru

def subir_csv_a_gcs(project_id, bucket_name, archivo_local, destino_gcs):
    
    try:    
        client = storage.Client(project=project_id)
        # Accedemos al bucket
        bucket = client.bucket(bucket_name)
        # Creamos un blob (archivo dentro del bucket)
        blob = bucket.blob(destino_gcs)
        # Subimos el archivo desde disco local
        blob.upload_from_filename(archivo_local)
        
        logging.info(f'Archivo de datos unificados subido a GCS: "gs://{bucket_name}/{destino_gcs}"')
    
    except Exception as e:
        logging.error(f'Error al cargar el archivo de datos unificados a GCS: {e}')

def crear_tabla_bigquery(project_id, dataset_id, table_id, schema):
    
    client = bigquery.Client(project=project_id)
    # Referencia a tabla destino
    table_ref = client.dataset(dataset_id).table(table_id)
    # Definimos la tabla con su esquema
    table = bigquery.Table(table_ref, schema=schema)
    
    try: 
        client.create_table(table)
        logging.info(f'Tabla de datos unificados creada en BigQuery: {table_id}.')
    except Conflict:
        logging.info(f'La tabla de datos unificados ya existe: {table_id}.')

def cargar_datos_tabla(project_id, dataset_id, table_id, gcs_uri):
    
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        # Configuración del trabajo de carga
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV, # El archivo es csv
            skip_leading_rows=1, # Saltamos el encabezado del archivo
            autodetect=False, # No dejamos que BigQuery adivine, usamos schema explícito
            write_disposition='WRITE_TRUNCATE' # Reemplaza la tabla si ya tenía datos 
        )

        # Ejecutamos el trabajo de carga desde el archivo CSV en GCS
        load_job = client.load_table_from_uri(
            gcs_uri,
            table_ref,
            job_config=job_config
        )
        load_job.result() # Espera a que termina el job
        
        logging.info(f'Datos unificados cargados en BigQuery: {table_id}')
    
    except Exception as e:
        logging.error(f'Error al cargar datos unificados en tabla {table_id}: {e}')

def ejecutar_carga_unificada():
    
    logging.info('=== Iniciando carga de datos unificados ===')
    
    # Ruta local
    ruta_local = os.path.join(DATA_PROCESSED_DIR, DATA_UNITY_PROCESSED)
    
    # Ruta destino dentro del bucket
    destino_gcs = f'processed_data/unificada/{DATA_UNITY_PROCESSED}'
    
    # Ruta completa en GCS para usar en la carga a BigQuery
    gcs_uri = f'gs://{BUCKET_NAME_LATAM}/{destino_gcs}'

    
    # Ejecutamos el pipeline de carga
    subir_csv_a_gcs(PROJECT_ID, BUCKET_NAME_LATAM, ruta_local, destino_gcs)
    # Creamos tabla
    crear_tabla_bigquery(PROJECT_ID, BQ_DATASET_LATAM, BQ_TABLE_UNITY, schema_bigquery)
    # Cargar datos a BigQuery
    cargar_datos_tabla(PROJECT_ID, BQ_DATASET_LATAM, BQ_TABLE_UNITY, gcs_uri)


def ejecutar_carga_gcp():
    
    logging.info('Iniciando carga de datos procesados a GCP...')
    
    try:
        ejecutar_carga_argentina()
        ejecutar_carga_uruguay()
        ejecutar_carga_peru()
        ejecutar_carga_unificada()
        
        logging.info('Carga de datos a GCP realizada con éxito')
    
    except Exception as e:
        logging.error(f'Error al cargar datos de países a GCP: {e}')
    
if __name__ == '__main__':
    ejecutar_carga_gcp()