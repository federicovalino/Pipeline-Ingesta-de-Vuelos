import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col
from pyspark.sql.types import (
    StructType, StructField,
    StringType, BooleanType, DoubleType, LongType, IntegerType
)
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)

BRONZE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "bronze", "vuelos"
)

# Schema explícito — evita que Spark mezcle tipos entre registros
SCHEMA = StructType([
    StructField("icao24",           StringType(),  True),
    StructField("callsign",         StringType(),  True),
    StructField("origin_country",   StringType(),  True),
    StructField("time_position",    LongType(),    True),
    StructField("last_contact",     LongType(),    True),
    StructField("longitude",        DoubleType(),  True),
    StructField("latitude",         DoubleType(),  True),
    StructField("baro_altitude",    DoubleType(),  True),
    StructField("on_ground",        BooleanType(), True),
    StructField("velocity",         DoubleType(),  True),
    StructField("true_track",       DoubleType(),  True),
    StructField("vertical_rate",    DoubleType(),  True),
    StructField("squawk",           StringType(),  True),
    StructField("timestamp_api",    LongType(),    True),
    StructField("ingested_at",      StringType(),  True),
])


def guardar_bronze(spark: SparkSession, vuelos: list[dict]) -> int:
    if not vuelos:
        logger.info("Bronze: no hay vuelos para guardar.")
        return 0

    # Usamos el schema explícito — sin inferencia
    df = spark.createDataFrame(vuelos, schema=SCHEMA)
    df = df.withColumn("ingested_at", to_timestamp(col("ingested_at")))

    if not DeltaTable.isDeltaTable(spark, BRONZE_PATH):
        logger.info("Bronze: primera ejecución, creando tabla Delta...")
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .save(BRONZE_PATH)
        )
        cantidad = df.count()
        logger.info(f"Bronze: tabla creada con {cantidad} registros.")
        return cantidad

    logger.info("Bronze: ejecutando MERGE incremental...")
    tabla_bronze = DeltaTable.forPath(spark, BRONZE_PATH)

    (
        tabla_bronze.alias("existente")
        .merge(
            df.alias("nuevo"),
            "existente.icao24 = nuevo.icao24 AND existente.timestamp_api = nuevo.timestamp_api"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    cantidad = df.count()
    logger.info(f"Bronze: MERGE completado, {cantidad} registros procesados.")
    return cantidad