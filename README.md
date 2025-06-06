# ⛽ Proyecto Combustibles LATAM
Este proyecto implementa un pipeline automatizado para descargar, procesar y analizar precios de combustibles en Argentina y LATAM. Es parte de una práctica autodidacta como Data Engineer, con foco en buenas prácticas de ingeniería de datos, versionado y modularidad.

## 🔍 Objetivo
Automatizar la ingesta, validación y posterior transformación de datos sobre precios de combustibles, generando una base confiable para análisis y visualizaciones.


# 🏗️ Etapas del Proyecto

### 1. Extracción de Datos
Scripts individuales por país, ubicados en `src/ingestion`. Se descargan datos de:

- **Argentina**: CSV desde el portal de Energía Abierta.
- **Uruguay**: HTML scrapeado desde sitio de ANCAP.
- **Perú**: PDF descargados desde sitio de MINEM.

Los archivos extraídos se almacenan en la carpeta `data/raw/`.

Log de procesos en `logs/ingestion.log`.
COnfiguración de logging centralizada en `src/utils/log_config.py`.


### 2. Transformación de Datos
Procesamiento y limpieza de los datos crudos en `src/transformation`, mediante scripts individuales por país:
- Renombrado y selección de columnas relevantes.
- Conversión de tipos de datos.
- Conversion de precios locales a USD, utilizando un script para obtener cotizaciones de la API Open Exchanges Rates.
- Eliminación de registros duplicados.
- Imputación de valores nulos mediante reglas específicas de negocio.
- Validación de la estructura y calidad de los datos procesados utilizando esquemas de `Pandera`.
- Generación de archivos `.csv` finales en `data/processed/`:
    - `preciosArgentina.csv`
    - `preciosUruguay.csv`
    - `preciosPeru.csv`
    - `combustibles_latam` (archivo con todos los datos de países unificados)

Log de procesos en `logs/transformation.log`.


### 3. Carga de Datos a Google Cloud Platform (GCP)

Automatización completa del proceso de carga a GCP, ubicada en `src/load`, estructurada en scripts individuales por país y un script general de orquestación. Esta etapa implementa la carga confiable de datos transformados en Google Cloud Storage (GCS) y BigQuery.

- **Google Cloud Storage (GCS)**:
    - Se crea automáticamente un bucket centralizado (`combustibles-latam-data`) si no existe.
    - Los archivos `.csv` procesados se cargan en carpetas organizadas por país y uno unificado:
      - `processed_data/argentina/preciosArgentina.csv`
      - `processed_data/uruguay/preciosUruguay.csv`
      - `processed_data/peru/preciosPeru.csv`
      - ``processed_data/unificada/combustibles_latam.csv`

- **BigQuery**:
    - Se crea automáticamente el dataset `combustibles_latam` si no existe.
    - Se crean las siguientes tablas (una por país y otra de datos unificados) con esquemas definidos explícitamente:
      - `precios_argentina`
      - `precios_uruguay`
      - `precios_peru`
      - `precios_unificados`
    - Los datos se cargan desde los archivos `.csv` alojados en GCS.
    - El esquema de cada tabla se define manualmente para asegurar consistencia en tipos de datos y control de calidad.

- **Automatización y estructura**:
    - Scripts separados por país (`Argentina.py`, `Uruguay.py`, `Peru.py`) y uno general (`Load.py`) que ejecuta la carga completa.
    - Uso de la librería oficial de Google (`google-cloud-storage` y `google-cloud-bigquery`) y configuración segura con variables de entorno mediante `.env`.
    - Autenticación gestionada localmente con credenciales de servicio (archivo `.json`).

Log de procesos centralizado en `logs/load.log`.
