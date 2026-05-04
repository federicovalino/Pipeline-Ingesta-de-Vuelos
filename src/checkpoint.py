import json
import os
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

CHECKPOINT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "checkpoints", "estado.json"
)


def leer_checkpoint() -> datetime | None:
    if not os.path.exists(CHECKPOINT_PATH):
        logger.info("Checkpoint: no existe, es la primera ejecución.")
        return None

    # Fix: verificar que el archivo no esté vacío
    if os.path.getsize(CHECKPOINT_PATH) == 0:
        logger.info("Checkpoint: archivo vacío, tratando como primera ejecución.")
        return None

    with open(CHECKPOINT_PATH) as f:
        data = json.load(f)

    if data.get("estado") != "exitoso":
        logger.warning("Checkpoint: la última ejecución no fue exitosa.")
        return None

    timestamp = datetime.fromisoformat(data["ultima_ejecucion"])
    logger.info(f"Checkpoint: última ejecución exitosa fue {timestamp}.")
    return timestamp


def guardar_checkpoint(registros_procesados: int):
    """
    Guarda el estado de la ejecución actual como exitoso.
    IMPORTANTE: llamar esto solo DESPUÉS de que todo el pipeline terminó bien.
    """
    os.makedirs(os.path.dirname(CHECKPOINT_PATH), exist_ok=True)

    data = {
        "ultima_ejecucion": datetime.now(timezone.utc).isoformat(),
        "registros_procesados": registros_procesados,
        "estado": "exitoso"
    }

    with open(CHECKPOINT_PATH, "w") as f:
        json.dump(data, f, indent=2)

    logger.info(f"Checkpoint guardado: {registros_procesados} registros procesados.")