# Importamos para crear esquema con Pandera para validación de datos
import pandera as pa
from pandera import Column, DataFrameSchema, Check

# Importamos para crear schema para tablas de BigQuery
from google.cloud import bigquery


import pandera as pa
from pandera import Column, DataFrameSchema, Check

schema_argentina = DataFrameSchema({
    'pais' : Column(pa.String, nullable=False),
    'fecha' : Column(pa.DateTime, nullable=False),
    'localidad' : Column(pa.String, nullable = False),
    'establecimiento' : Column(pa.String, nullable=True),
    'producto' : Column(pa.String, nullable=False),
    'precio_litro' : Column(pa.Float, Check.ge(0), nullable=False),
    'precio_usd_litro' : Column(pa.Float, Check.ge(0), nullable=False)
})

schema_uruguay = DataFrameSchema({
    'pais' : Column(pa.String, nullable=False),
    'fecha' : Column(pa.DateTime, nullable=False),
    'localidad' : Column(pa.String, nullable = False),
    'establecimiento' : Column(pa.String, nullable=True),
    'producto' : Column(pa.String, nullable=False),
    'precio_litro' : Column(pa.Float, Check.ge(0), nullable=False),
    'precio_usd_litro' : Column(pa.Float, Check.ge(0), nullable=False)
})

schema_peru = DataFrameSchema({
    'pais' : Column(pa.String, nullable=False),
    'fecha' : Column(pa.DateTime, nullable=False),
    'localidad' : Column(pa.String, nullable = False),
    'establecimiento' : Column(pa.String, nullable=True),
    'producto' : Column(pa.String, nullable=False),
    'precio_litro' : Column(pa.Float, Check.ge(0), nullable=False),
    'precio_usd_litro' : Column(pa.Float, Check.ge(0), nullable=False)
})

 # Definimos el schema esperado por la tabla en BigQuery
schema_bigquery = [
    bigquery.SchemaField('pais', 'STRING'), 
    bigquery.SchemaField('fecha', 'DATE'),
    bigquery.SchemaField('localidad', 'STRING'),
    bigquery.SchemaField('establecimiento', 'STRING'),
    bigquery.SchemaField('producto', 'STRING'),
    bigquery.SchemaField('precio_litro', 'FLOAT'),
    bigquery.SchemaField('precio_usd_litro', 'FLOAT')
]