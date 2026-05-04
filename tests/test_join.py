# tests/test_join.py
import pytest
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.spark_session import get_spark_session

GOLD_PATH   = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "gold", "vuelos_enriquecidos")
SILVER_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "silver", "vuelos")


@pytest.fixture(scope="module")
def spark():
    s = get_spark_session("tests")
    yield s
    s.stop()


def test_gold_tiene_todos_los_vuelos_de_silver(spark):
    """
    Gold debe tener exactamente los mismos vuelos que Silver.
    El JOIN no debe perder ni agregar filas — es un left join por icao24.
    """
    df_silver = spark.read.format("delta").load(SILVER_PATH)
    df_gold   = spark.read.format("delta").load(GOLD_PATH)

    assert df_silver.count() == df_gold.count(), (
        f"Gold tiene {df_gold.count()} filas pero Silver tiene {df_silver.count()}. "
        f"El JOIN modificó la cantidad de registros."
    )


def test_gold_tiene_columnas_de_enriquecimiento(spark):
    """
    Gold debe tener las columnas que vienen del JOIN con aircraftDatabase.
    Si no existen, el JOIN no se ejecutó correctamente.
    """
    df = spark.read.format("delta").load(GOLD_PATH)
    columnas_esperadas = {"fabricante", "modelo", "tipo_aeronave", "operador"}

    columnas_existentes = set(df.columns)
    faltantes = columnas_esperadas - columnas_existentes

    assert not faltantes, f"Gold le faltan columnas del JOIN: {faltantes}"


def test_join_no_genera_nulos_en_campos_clave(spark):
    """
    Los campos que vienen de Silver (no del JOIN) nunca deben ser null en Gold.
    Si son null, significa que el JOIN perdió datos de Silver.
    """
    from pyspark.sql.functions import col

    df = spark.read.format("delta").load(GOLD_PATH)

    campos_clave = ["icao24", "origin_country", "flight_status", "timestamp_api"]

    for campo in campos_clave:
        nulos = df.filter(col(campo).isNull()).count()
        assert nulos == 0, (
            f"El campo '{campo}' tiene {nulos} valores null en Gold — el JOIN perdió datos de Silver"
        )


def test_gold_icao24_subset_de_silver(spark):
    """
    Todos los icao24 de Gold deben existir en Silver.
    Verifica que no se colaron registros extraños en el JOIN.
    """
    df_silver = spark.read.format("delta").load(SILVER_PATH)
    df_gold   = spark.read.format("delta").load(GOLD_PATH)

    icao_silver = {row["icao24"] for row in df_silver.select("icao24").distinct().collect()}
    icao_gold   = {row["icao24"] for row in df_gold.select("icao24").distinct().collect()}

    extraños = icao_gold - icao_silver
    assert not extraños, f"Gold tiene icao24 que no existen en Silver: {extraños}"