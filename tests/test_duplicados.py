import pytest
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.spark_session import get_spark_session

BRONZE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "bronze", "vuelos")
SILVER_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "silver", "vuelos")
GOLD_PATH   = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "gold", "vuelos_enriquecidos")


@pytest.fixture(scope="module")
def spark():
    s = get_spark_session("tests")
    yield s
    s.stop()


def test_bronze_sin_duplicados(spark):
    """
    En Bronze, la combinación icao24 + timestamp_api debe ser única.
    Es la clave del MERGE — si hay duplicados, el incremental está roto.
    """
    df = spark.read.format("delta").load(BRONZE_PATH)

    total = df.count()
    unicos = df.select("icao24", "timestamp_api").distinct().count()

    assert total == unicos, (
        f"Bronze tiene duplicados: {total} filas pero solo {unicos} combinaciones únicas de icao24+timestamp_api"
    )


def test_silver_sin_duplicados(spark):
    """
    Silver también debe ser único por icao24 + timestamp_api.
    """
    df = spark.read.format("delta").load(SILVER_PATH)

    total = df.count()
    unicos = df.select("icao24", "timestamp_api").distinct().count()

    assert total == unicos, (
        f"Silver tiene duplicados: {total} filas pero solo {unicos} combinaciones únicas de icao24+timestamp_api"
    )


def test_gold_sin_duplicados(spark):
    """
    Gold también debe ser único por icao24 + timestamp_api.
    """
    df = spark.read.format("delta").load(GOLD_PATH)

    total = df.count()
    unicos = df.select("icao24", "timestamp_api").distinct().count()

    assert total == unicos, (
        f"Gold tiene duplicados: {total} filas pero solo {unicos} combinaciones únicas de icao24+timestamp_api"
    )


def test_silver_tiene_registros(spark):
    """
    Silver no debe estar vacío — si el pipeline corrió, tiene que haber datos.
    """
    df = spark.read.format("delta").load(SILVER_PATH)
    assert df.count() > 0, "Silver está vacío — el pipeline no procesó ningún registro"


def test_flight_status_valores_validos(spark):
    """
    El campo flight_status en Silver solo puede tener valores conocidos.
    Verifica que la lógica de clasificación no produjo valores inesperados.
    """
    from pyspark.sql.functions import col

    df = spark.read.format("delta").load(SILVER_PATH)
    valores_validos = {"en_vuelo", "en_tierra", "despegando_o_aterrizando", "desconocido"}

    valores_encontrados = {
        row["flight_status"]
        for row in df.select("flight_status").distinct().collect()
    }

    invalidos = valores_encontrados - valores_validos
    assert not invalidos, f"flight_status tiene valores inesperados: {invalidos}"