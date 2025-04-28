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