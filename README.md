# Pipeline de Ingesta de Vuelos — Aeropuertos Uruguay

Pipeline de ingesta y transformación de datos de vuelos en tiempo real sobre el espacio aéreo uruguayo, construido con PySpark y Delta Lake siguiendo arquitectura Medallion (Bronze / Silver / Gold).

---

## Contexto de negocio
Aeropuertos Uruguay opera los principales aeropuertos del país, incluyendo el 
Aeropuerto Internacional de Carrasco, Laguna del Sauce y Rivera, entre otros. 
La gestión eficiente del espacio aéreo requiere visibilidad en tiempo real sobre 
qué aeronaves están sobrevolando territorio uruguayo, su estado, origen y características.

Este pipeline resuelve ese problema capturando posiciones de vuelo en tiempo real 
desde OpenSky Network y construyendo una fuente de datos confiable, incremental y 
enriquecida que permite responder preguntas como:

- ¿Cuántos vuelos están sobrevolando Uruguay en este momento?
- ¿De qué países provienen las aeronaves?
- ¿Qué aeronaves están despegando, aterrizando o en vuelo de crucero?
- ¿Qué fabricante y operador corresponde a cada aeronave identificada?

Los datos procesados en la capa Gold están listos para ser consumidos por 
herramientas de Business Intelligence como Power BI o Tableau, o por sistemas 
operativos que requieran información actualizada del espacio aéreo.

---

## Descripción general

El pipeline consume la API pública de [OpenSky Network](https://opensky-network.org/) para obtener posiciones de aeronaves en tiempo real sobre Uruguay. Los datos se persisten incrementalmente en tres capas Delta Lake y se enriquecen con información de aeronaves mediante un archivo estático.

---

## Arquitectura

```
API OpenSky
     ↓
  Bronze (raw)       → JSON crudo persistido en Delta Lake
     ↓
  Silver (clean)     → Tipos correctos, timestamps legibles, flight_status
     ↓
  Gold (curated)    → JOIN con aircraftDatabase.csv → fabricante, modelo, operador
```

---

## Estructura del proyecto

```
proyecto-aeropuertos/
├── src/
│   ├── spark_session.py       # Inicialización de SparkSession con Delta
│   ├── ingesta.py             # Llamada a API con retries y backoff exponencial
│   ├── bronze.py              # Persistencia de datos crudos en Delta
│   ├── silver.py              # Transformaciones, tipos, flight_status
│   ├── gold.py                # JOIN con archivo estático
│   └── checkpoint.py          # Gestión de estado incremental
├── static/
│   └── aircraftDatabase.csv   # Muestra de 199 aeronaves (región + Europa + USA)
├── data/
│   ├── bronze/vuelos/         # Tabla Delta capa Bronze
│   ├── silver/vuelos/         # Tabla Delta capa Silver
│   ├── gold/vuelos_enriquecidos/  # Tabla Delta capa Gold
│   └── checkpoints/
│       └── estado.json        # Estado de la última ejecución
├── tests/
│   ├── test_duplicados.py
│   └── test_join.py
├── main.py                    # Punto de entrada del pipeline
├── requirements.txt
└── README.md
```

---

## Instalación y ejecución

### Requisitos

- Python 3.11
- Java 17 (requerido por Spark)
- Hadoop winutils (solo Windows)

### Instalación de dependencias

```bash
py -3.11 -m pip install -r requirements.txt
```

### Ejecutar el pipeline

```bash
py -3.11 main.py
```

Cada ejecución de `main.py` corre el pipeline completo: ingesta desde la API, persistencia en Bronze, transformación a Silver, enriquecimiento en Gold y actualización del checkpoint.

### Ejecutar los tests

> **Importante:** los tests asumen que las tablas Delta ya existen en `data/`. 
> Es necesario correr el pipeline al menos una vez antes de ejecutar los tests.

```bash
py -3.11 -m pytest tests/ -v
```

---

## Gestión del estado incremental (Checkpointing)

Este es el mecanismo central del pipeline para garantizar que cada ejecución procese únicamente los datos nuevos desde la última ejecución exitosa.

### Estructura del checkpoint

El estado se persiste en `data/checkpoints/estado.json`:

```json
{
  "ultima_ejecucion": "2026-05-03T22:12:40.134910+00:00",
  "registros_procesados": 12,
  "estado": "exitoso"
}
```

### Flujo de cada ejecución

```
1. Leer checkpoint
        ↓
   ¿Existe y estado = "exitoso"?
        ↓ Sí                        ↓ No
   Carga incremental            Carga completa
   desde ultima_ejecucion       desde el inicio
        ↓
2. Llamar a la API (con retries)
        ↓
3. MERGE en Bronze  →  solo inserta registros nuevos
        ↓
4. MERGE en Silver  →  inserta o actualiza
        ↓
5. MERGE en Gold    →  inserta o actualiza
        ↓
6. Guardar checkpoint con estado = "exitoso"
   ← Solo llega acá si TODO el pipeline completó sin errores
```

### Garantías del diseño

**Idempotencia** — el pipeline usa MERGE en todas las capas en lugar de append o overwrite. Si la misma ejecución corre dos veces con los mismos datos, el resultado es idéntico — sin duplicados.

**Checkpoint tardío** — el checkpoint se guarda únicamente después de que todas las capas completaron exitosamente. Si el pipeline falla en Silver o Gold, el checkpoint no avanza y la próxima ejecución reintenta desde el mismo punto.

**Primera ejecución** — si no existe checkpoint o el archivo está vacío, el pipeline realiza una carga completa sin filtro de fecha.

### Evolución a producción

En un entorno productivo el checkpoint en archivo JSON se reemplazaría por una tabla Delta dedicada, lo que permite auditoría completa del historial de ejecuciones y coordinación entre múltiples pipelines.

---

## Manejo de errores

La capa de ingesta implementa reintentos con backoff exponencial:

- Hasta 3 reintentos ante timeout o error de conexión
- Ante HTTP 429 (rate limiting): espera de 5s → 10s → 20s
- Si todos los reintentos fallan, el pipeline lanza una excepción y el checkpoint no avanza

---

## Transformaciones Silver

| Campo | Transformación |
|---|---|
| `time_position` | Unix epoch (Long) → Timestamp legible |
| `last_contact` | Unix epoch (Long) → Timestamp legible |
| `timestamp_api` | Unix epoch (Long) → Timestamp legible |
| `baro_altitude` | Redondeado a 2 decimales |
| `velocity` | Redondeado a 2 decimales |
| `longitude / latitude` | Redondeados a 4 decimales |
| `callsign` | Null → "DESCONOCIDO" |
| `flight_status` | Clasificación: `en_vuelo` / `en_tierra` / `despegando_o_aterrizando` |

---

## Schema Evolution

Todas las capas se escriben con `mergeSchema: true`. Si la API agrega nuevos campos en el futuro, el pipeline los absorbe automáticamente sin romper las tablas existentes ni requerir migraciones manuales.

---

## Enriquecimiento en Gold

El JOIN en Gold conecta cada vuelo con su aeronave mediante `icao24` — el identificador único físico del avión, equivalente a una patente.

**Fuente estática:** `aircraftDatabase.csv` — muestra de 199 aeronaves de aerolíneas de Sudamérica, Europa y USA con rutas relevantes hacia Uruguay (LATAM, Flybondi, Iberia, Air Europa, KLM, Delta, American, entre otras).

**En producción** se usaría la base completa de OpenSky (~520k registros), aumentando significativamente la tasa de match del JOIN. La lógica del pipeline es independiente del tamaño del dataset.

---

## Tests

| Test | Descripción |
|---|---|
| `test_bronze_sin_duplicados` | `icao24 + timestamp_api` es único en Bronze |
| `test_silver_sin_duplicados` | `icao24 + timestamp_api` es único en Silver |
| `test_gold_sin_duplicados` | `icao24 + timestamp_api` es único en Gold |
| `test_silver_tiene_registros` | Silver no está vacío tras el pipeline |
| `test_flight_status_valores_validos` | `flight_status` solo tiene valores esperados |
| `test_gold_tiene_todos_los_vuelos_de_silver` | El JOIN no pierde ni agrega filas |
| `test_gold_tiene_columnas_de_enriquecimiento` | Gold tiene las columnas del JOIN |
| `test_join_no_genera_nulos_en_campos_clave` | Campos de Silver no son null en Gold |
| `test_gold_icao24_subset_de_silver` | Gold no tiene `icao24` ajenos a Silver |

---

## Decisiones técnicas

**¿Por qué OpenSky?** API pública, sin registro, con datos reales de vuelos sobre Uruguay en tiempo real. Los datos cambian constantemente — ideal para demostrar carga incremental.

**¿Por qué archivo JSON para el checkpoint?** Suficiente para el alcance de esta prueba y simple de auditar. En producción se migraría a una tabla Delta dedicada.

**¿Por qué no Docker?** El foco de la prueba es el pipeline de datos. En producción se containerizaría con una imagen o se desplegaría en Databricks.