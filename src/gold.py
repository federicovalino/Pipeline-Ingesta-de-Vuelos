import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)

SILVER_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "silver", "vuelos"
)

GOLD_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "gold", "vuelos_enriquecidos"
)

AIRCRAFT_CSV = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "static", "aircraftDatabase.csv"
)


def transformar_gold(spark: SparkSession) -> int:
    """
    Lee Silver, enriquece con datos de aeronave (fabricante, modelo, operador)
    mediante JOIN con aircraftDatabase.csv, y persiste en Gold con MERGE.
    """

    # --- Leer Silver ---
    logger.info("Gold: leyendo desde Silver...")
    df_silver = spark.read.format("delta").load(SILVER_PATH)

    # --- Leer aircraftDatabase ---
    logger.info("Gold: leyendo aircraftDatabase...")
    df_aircraft = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(AIRCRAFT_CSV)
        .select(
            trim(upper(col("icao24"))).alias("icao24_key"),
            col("manufacturername").alias("fabricante"),
            col("model").alias("modelo"),
            col("typecode").alias("tipo_aeronave"),
            col("owner").alias("operador"),
            col("operatorcallsign").alias("callsign_operador"),
            col("operatoriata").alias("iata_operador"),
        )
    )

    # --- Preparar clave de JOIN ---
    df_silver = df_silver.withColumn(
        "icao24_key", trim(upper(col("icao24")))
    )

    # --- LEFT JOIN por icao24 ---
    # Left para no perder vuelos que no estén en el dataset de aeronaves
    logger.info("Gold: ejecutando JOIN con aircraftDatabase...")
    df_gold = (
        df_silver.alias("v")
        .join(df_aircraft.alias("a"), on="icao24_key", how="left")
        .select(
            # Identidad del vuelo
            col("v.icao24"),
            col("v.callsign"),
            col("v.origin_country"),

            # Posición y estado
            col("v.longitude"),
            col("v.latitude"),
            col("v.baro_altitude"),
            col("v.velocity"),
            col("v.vertical_rate"),
            col("v.true_track"),
            col("v.flight_status"),
            col("v.on_ground"),
            col("v.squawk"),

            # Tiempos
            col("v.timestamp_api"),
            col("v.ingested_at"),

            # Enriquecimiento — aeronave
            col("a.fabricante"),
            col("a.modelo"),
            col("a.tipo_aeronave"),
            col("a.operador"),
            col("a.callsign_operador"),
            col("a.iata_operador"),
        )
    )

    # --- Persistir en Gold ---
    if not DeltaTable.isDeltaTable(spark, GOLD_PATH):
        logger.info("Gold: primera ejecución, creando tabla Delta...")
        (
            df_gold.write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .save(GOLD_PATH)
        )
        cantidad = df_gold.count()
        logger.info(f"Gold: tabla creada con {cantidad} registros.")
        return cantidad

    logger.info("Gold: ejecutando MERGE...")
    tabla_gold = DeltaTable.forPath(spark, GOLD_PATH)

    (
        tabla_gold.alias("existente")
        .merge(
            df_gold.alias("nuevo"),
            "existente.icao24 = nuevo.icao24 AND existente.timestamp_api = nuevo.timestamp_api"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    cantidad = df_gold.count()
    logger.info(f"Gold: MERGE completado, {cantidad} registros en tabla.")
    return cantidad