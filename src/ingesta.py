import requests
import time
import logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

API_URL = "https://opensky-network.org/api/states/all"

# Bounding box de Uruguay — solo traemos vuelos sobre Uruguay
PARAMS = {
    "lamin": -35.0,
    "lomin": -58.5,
    "lamax": -30.0,
    "lomax": -53.0,
}

def llamar_api(max_reintentos: int = 3, espera_inicial: int = 5) -> dict:
    """
    Llama a la API de OpenSky con reintentos y backoff exponencial.
    Retorna el JSON crudo o lanza excepción si todos los reintentos fallan.
    """
    intento = 0

    while intento < max_reintentos:
        try:
            logger.info(f"Llamando a la API (intento {intento + 1}/{max_reintentos})...")
            response = requests.get(API_URL, params=PARAMS, timeout=15)

            if response.status_code == 200:
                logger.info("API respondió OK.")
                return response.json()

            elif response.status_code == 429:
                # Rate limiting — esperamos más
                espera = espera_inicial * (2 ** intento)
                logger.warning(f"Rate limit alcanzado. Esperando {espera}s...")
                time.sleep(espera)

            else:
                logger.warning(f"Status inesperado: {response.status_code}. Reintentando...")
                time.sleep(espera_inicial * (2 ** intento))

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout en intento {intento + 1}. Reintentando...")
            time.sleep(espera_inicial * (2 ** intento))

        except requests.exceptions.ConnectionError:
            logger.warning(f"Error de conexión en intento {intento + 1}. Reintentando...")
            time.sleep(espera_inicial * (2 ** intento))

        intento += 1

    raise Exception(f"La API falló después de {max_reintentos} reintentos.")


def parsear_estados(raw: dict) -> list[dict]:
    if not raw.get("states"):
        logger.info("La API no devolvió estados.")
        return []

    timestamp_api = raw.get("time", 0)
    ingested_at = datetime.now(timezone.utc).isoformat()

    vuelos = []
    for state in raw["states"]:
        vuelos.append({
            "icao24":           state[0],
            "callsign":         state[1].strip() if state[1] else None,
            "origin_country":   state[2],
            "time_position":    int(state[3])    if state[3]  is not None else None,
            "last_contact":     int(state[4])    if state[4]  is not None else None,
            "longitude":        float(state[5])  if state[5]  is not None else None,
            "latitude":         float(state[6])  if state[6]  is not None else None,
            "baro_altitude":    float(state[7])  if state[7]  is not None else None,
            "on_ground":        bool(state[8])   if state[8]  is not None else None,
            "velocity":         float(state[9])  if state[9]  is not None else None,
            "true_track":       float(state[10]) if state[10] is not None else None,
            "vertical_rate":    float(state[11]) if state[11] is not None else None,
            "squawk":           state[14],
            "timestamp_api":    int(timestamp_api),
            "ingested_at":      ingested_at,
        })

    logger.info(f"Parseados {len(vuelos)} vuelos.")
    return vuelos