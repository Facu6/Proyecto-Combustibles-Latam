from google.cloud import storage, bigquery
from google.api_core.exceptions import Conflict, NotFound
import os

from src.utils.log_config import logging
from config.config import(
    PROJECT_ID,
    BQ_DATASET_LATAM,
    BUCKET_NAME_LATAM,
    BQ_TABLE_PE,
    DATA_PROCESSED_DIR,
    PE_DATASET_FILENAME_PROCESSED
)


def crear_bucket(project_id, bucket_name):
    
    client = storage.Client(project=project_id)
    try:
        client.create_bucket(bucket_name)
        logging.info(f'Bucket creado con éxito: {bucket_name}.')
    except Conflict:
        logging.info(f'El bucket ya existe: {bucket_name}.') 

def subir_csv_a_gcs(project_id, bucket_name, archivo_local, destino_gcs):
    
    try:    
        client = storage.Client(project=project_id)
        # Accedemos al bucket
        bucket = client.bucket(bucket_name)
        # Creamos un blob (archivo dentro del bucket)
        blob = bucket.blob(destino_gcs)
        # Subimos el archivo desde disco local
        blob.upload_from_filename(archivo_local)
        
        logging.info(f'Archivo de Perú subido a GCS: "gs://{bucket_name}/{destino_gcs}"')
    
    except Exception as e:
        logging.error(f'Error al cargar el archivo de Perú a GCS: {e}')

def crear_dataset(project_id, dataset_id):
    
    client = bigquery.Client(project=project_id)
    dataset_ref = bigquery.Dataset(f'{project_id}.{dataset_id}')
    
    try:
        client.get_dataset(dataset_ref)
        logging.info(f'El dataset ya existe: {dataset_id}')
    except NotFound:
        try:
            client.create_dataset(dataset_ref)
            logging.info(f'Dataset creado con éxito: {dataset_id}.')        
        except Exception as e:
            logging.error(f'Error en la creación del dataset: {e}')
            raise
        
def crear_tabla_bigquery(project_id, dataset_id, table_id, schema):
    
    client = bigquery.Client(project=project_id)
    # Referencia a tabla destino
    table_ref = client.dataset(dataset_id).table(table_id)
    # Definimos la tabla con su esquema
    table = bigquery.Table(table_ref, schema=schema)
    
    try: 
        client.create_table(table)
        logging.info(f'Tabla de Perú creada en BigQuery: {table_id}.')
    except Conflict:
        logging.info(f'La tabla de Perú ya existe: {table_id}.')

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
        
        logging.info(f'Datos de Perú cargado en BigQuery: {table_id}')
    
    except Exception as e:
        logging.error(f'Error al cargar datos de Perú en tabla {table_id}: {e}')

def ejecutar_carga_peru():
    
    logging.info('=== Iniciando carga de datos de Perú ===')
    # Ruta local del archivo procesado
    ruta_local = os.path.join(DATA_PROCESSED_DIR, PE_DATASET_FILENAME_PROCESSED)
    
    # Ruta destino dentro del bucket
    destino_gsc = f'processed_data/peru/{PE_DATASET_FILENAME_PROCESSED}'
    
    # Ruta completa en GCS para usar en la carga a BigQuery
    gcs_uri = f'gs://{BUCKET_NAME_LATAM}/{destino_gsc}'
    
    # Ejecutamos el pipeline de carga
    crear_bucket(PROJECT_ID, BUCKET_NAME_LATAM)
    subir_csv_a_gcs(PROJECT_ID, BUCKET_NAME_LATAM, ruta_local, destino_gsc)
    
    # Definimos el schema esperado por la tabla en BigQuery
    schema = [
        bigquery.SchemaField('pais', 'STRING'), 
        bigquery.SchemaField('fecha', 'DATE'),
        bigquery.SchemaField('localidad', 'STRING'),
        bigquery.SchemaField('establecimiento', 'STRING'),
        bigquery.SchemaField('producto', 'STRING'),
        bigquery.SchemaField('precio_litro', 'FLOAT'),
        bigquery.SchemaField('precio_usd_litro', 'FLOAT')
    ]
    
    # Creamos dataset y tabla
    crear_dataset(PROJECT_ID, BQ_DATASET_LATAM)
    crear_tabla_bigquery(PROJECT_ID, BQ_DATASET_LATAM, BQ_TABLE_PE, schema)
    
    # Cargar datos a BigQuery
    cargar_datos_tabla(PROJECT_ID, BQ_DATASET_LATAM, BQ_TABLE_PE, gcs_uri)
    

if __name__ == '__main__':   
    ejecutar_carga_peru()
    
    
        


