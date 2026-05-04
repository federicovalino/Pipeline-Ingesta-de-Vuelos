import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_unixtime, to_timestamp, when, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, BooleanType, DoubleType, LongType, TimestampType
)
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)

BRONZE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "bronze", "vuelos"
)

SILVER_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "silver", "vuelos"
)


def transformar_silver(spark: SparkSession) -> int:
    """
    Lee Bronze, aplica transformaciones y persiste en Silver con MERGE.
    Retorna cantidad de registros procesados.
    """

    # --- Leer Bronze ---
    logger.info("Silver: leyendo desde Bronze...")
    df = spark.read.format("delta").load(BRONZE_PATH)

    # --- Transformaciones ---

    # 1. Convertir timestamps Unix a timestamp legible
    #    time_position y last_contact vienen como segundos epoch (Long)
    df = df.withColumn(
        "time_position_ts",
        to_timestamp(from_unixtime(col("time_position")))
    ).withColumn(
        "last_contact_ts",
        to_timestamp(from_unixtime(col("last_contact")))
    ).withColumn(
        "timestamp_api_ts",
        to_timestamp(from_unixtime(col("timestamp_api")))
    )

    # 2. Redondear decimales — coordenadas y velocidad a 4 decimales
    df = df.withColumn("longitude",     spark_round(col("longitude"),     4)) \
           .withColumn("latitude",      spark_round(col("latitude"),      4)) \
           .withColumn("baro_altitude", spark_round(col("baro_altitude"), 2)) \
           .withColumn("velocity",      spark_round(col("velocity"),      2)) \
           .withColumn("vertical_rate", spark_round(col("vertical_rate"), 2))

    # 3. Clasificar estado del vuelo
    df = df.withColumn(
        "flight_status",
        when(col("on_ground") == True, "en_tierra")
        .when(col("baro_altitude") < 1000, "despegando_o_aterrizando")
        .when(col("baro_altitude") >= 1000, "en_vuelo")
        .otherwise("desconocido")
    )

    # 4. Limpiar callsign — puede venir con espacios o None
    df = df.withColumn(
        "callsign",
        when(col("callsign").isNull(), "DESCONOCIDO")
        .otherwise(col("callsign"))
    )

    # 5. Seleccionar y renombrar columnas finales — Silver solo tiene lo que necesita
    df_silver = df.select(
        col("icao24"),
        col("callsign"),
        col("origin_country"),
        col("longitude"),
        col("latitude"),
        col("baro_altitude"),
        col("velocity"),
        col("vertical_rate"),
        col("true_track"),
        col("on_ground"),
        col("squawk"),
        col("flight_status"),
        col("time_position_ts").alias("time_position"),
        col("last_contact_ts").alias("last_contact"),
        col("timestamp_api_ts").alias("timestamp_api"),
        col("ingested_at"),
    )

    # --- Schema Evolution: si Silver no existe la creamos ---
    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        logger.info("Silver: primera ejecución, creando tabla Delta...")
        (
            df_silver.write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")  # permite agregar columnas nuevas en el futuro
            .save(SILVER_PATH)
        )
        cantidad = df_silver.count()
        logger.info(f"Silver: tabla creada con {cantidad} registros.")
        return cantidad

    # --- MERGE incremental desde Bronze ---
    logger.info("Silver: ejecutando MERGE...")
    tabla_silver = DeltaTable.forPath(spark, SILVER_PATH)

    (
        tabla_silver.alias("existente")
        .merge(
            df_silver.alias("nuevo"),
            "existente.icao24 = nuevo.icao24 AND existente.timestamp_api = nuevo.timestamp_api"
        )
        .whenMatchedUpdateAll()    # Silver SÍ actualiza — si el dato mejoró, lo reflejamos
        .whenNotMatchedInsertAll()
        .execute()
    )

    cantidad = df_silver.count()
    logger.info(f"Silver: MERGE completado, {cantidad} registros en tabla.")
    return cantidad