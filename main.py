import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

from src.spark_session import get_spark_session
from src.ingesta import llamar_api, parsear_estados
from src.bronze import guardar_bronze
from src.silver import transformar_silver
from src.gold import transformar_gold
from src.checkpoint import leer_checkpoint, guardar_checkpoint

def main():
    # 1. Spark
    spark = get_spark_session()

    # 2. Checkpoint
    ultimo = leer_checkpoint()

    # 3. API
    raw = llamar_api()
    vuelos = parsear_estados(raw)

    if not vuelos:
        print("No hay vuelos sobre Uruguay en este momento. Reintentá en unos minutos.")
        return

    # 4. Bronze
    cantidad = guardar_bronze(spark, vuelos)

    # 5. Silver
    transformar_silver(spark)

    # 6. Gold
    transformar_gold(spark)

    # 7. Checkpoint — solo si todo salió bien
    guardar_checkpoint(cantidad)

    print("\n✅ Pipeline completado exitosamente.")

if __name__ == "__main__":
    main()