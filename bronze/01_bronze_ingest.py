# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — ingesta de currículums
# MAGIC
# MAGIC **Fuente:** [Resume Dataset (Kaggle, snehaanbhawal)](https://www.kaggle.com/datasets/snehaanbhawal/resume-dataset) —
# MAGIC ~2400 currículums reales en PDF, organizados en carpetas por categoría laboral (HR, IT,
# MAGIC Healthcare, Sales, Finance, Engineering, y otras — 24 categorías). El dataset trae también un
# MAGIC CSV con el texto ya extraído (`Resume_str`) que ignoramos a propósito: leemos los PDFs
# MAGIC directamente para que la ingesta sea extracción real de un formato no estructurado.
# MAGIC
# MAGIC **Columnas que produce esta capa:** `resume_id`, `category`, `raw_text`, `file_path`,
# MAGIC `_ingested_at`, `_source`. Nada de limpieza acá — `raw_text` sale tal cual de la extracción del
# MAGIC PDF, con sus artefactos de espacios/saltos de línea. Silver se encarga de normalizar.
# MAGIC
# MAGIC **Pregunta de negocio que esto alimenta:** ¿qué categorías de currículums muestran más señales
# MAGIC de experiencia y liderazgo, y qué candidatos destacan dentro de cada una?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Catalog, schemas y Volume de landing
# MAGIC
# MAGIC Proyecto propio con su catalog dedicado (mismo patrón que `instacart`), para no chocar con
# MAGIC otros estudiantes ni con otros proyectos. `IF NOT EXISTS` en todo — correr esta celda de nuevo
# MAGIC no rompe nada.

# COMMAND ----------

CATALOG = "recruitment_copilot"
LANDING_VOLUME = "landing"
LANDING_PATH = f"/Volumes/{CATALOG}/bronze/{LANDING_VOLUME}"
CHECKPOINT_PATH = f"{LANDING_PATH}/_checkpoints/bronze_resumes"
BRONZE_TABLE = f"{CATALOG}.bronze.bronze_resumes"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.{LANDING_VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Descargar el dataset a la carpeta de landing
# MAGIC
# MAGIC El dataset vive en Kaggle, no en Databricks — así que primero lo bajamos al Volume, y recién
# MAGIC después Auto Loader lo lee desde ahí. Credenciales desde el secret scope `kaggle` (creado con
# MAGIC `setup_secrets.py` en la raíz del repo — nunca hardcodeadas acá).
# MAGIC
# MAGIC Idempotente a nivel de landing: si la carpeta ya tiene archivos, no vuelve a descargar.

# COMMAND ----------

# MAGIC %pip install -q kaggle pypdf
dbutils.library.restartPython()

# COMMAND ----------

CATALOG = "recruitment_copilot"
LANDING_VOLUME = "landing"
LANDING_PATH = f"/Volumes/{CATALOG}/bronze/{LANDING_VOLUME}"
CHECKPOINT_PATH = f"{LANDING_PATH}/_checkpoints/bronze_resumes"
BRONZE_TABLE = f"{CATALOG}.bronze.bronze_resumes"

import os

os.environ["KAGGLE_USERNAME"] = dbutils.secrets.get("kaggle", "username")
os.environ["KAGGLE_KEY"] = dbutils.secrets.get("kaggle", "key")

already_downloaded = len(dbutils.fs.ls(LANDING_PATH)) > 0

if not already_downloaded:
    import subprocess
    subprocess.run(
        ["kaggle", "datasets", "download", "-d", "snehaanbhawal/resume-dataset",
         "-p", LANDING_PATH, "--unzip"],
        check=True,
    )
    print("Descarga completa.")
else:
    print("La carpeta de landing ya tiene contenido — se omite la descarga.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificar la estructura real de carpetas
# MAGIC
# MAGIC **Importante:** no asumimos de antemano en qué subcarpeta exacta quedan los PDFs por
# MAGIC categoría — el zip de Kaggle puede desempacar en un nivel de anidamiento distinto al esperado.
# MAGIC Correr esta celda y **ajustar `CATEGORY_ROOT` abajo** según lo que se vea acá antes de seguir.

# COMMAND ----------

# MAGIC %sh
# MAGIC find /Volumes/recruitment_copilot/bronze/landing -maxdepth 4 | head -50

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Auto Loader — lectura incremental de los PDFs
# MAGIC
# MAGIC `binaryFile` porque no leemos texto tabular, leemos el PDF crudo como bytes. La categoría sale
# MAGIC del nombre de la carpeta inmediata (`.../<category>/<archivo>.pdf`); ajustar `CATEGORY_ROOT` si
# MAGIC la celda anterior mostró otra profundidad.

# COMMAND ----------

from pyspark.sql.functions import (
    col, current_timestamp, lit, regexp_extract, udf,
)
from pyspark.sql.types import StringType
from pypdf import PdfReader
import io

# TODO: ajustar si la celda 3 mostró que los PDFs quedan en otra profundidad
CATEGORY_ROOT = f"{LANDING_PATH}/data/data"


def extract_pdf_text(content: bytes) -> str:
    try:
        reader = PdfReader(io.BytesIO(content))
        return "\n".join((page.extract_text() or "") for page in reader.pages)
    except Exception:
        return None


extract_pdf_text_udf = udf(extract_pdf_text, StringType())

raw_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("pathGlobFilter", "*.pdf")
    .load(CATEGORY_ROOT)
)

bronze_stream = (
    raw_stream
    .withColumn("resume_id", regexp_extract(col("path"), r"([^/]+)\.pdf$", 1))
    .withColumn("category", regexp_extract(col("path"), r"/([^/]+)/[^/]+\.pdf$", 1))
    .withColumn("raw_text", extract_pdf_text_udf(col("content")))
    .withColumn("file_path", col("path"))
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source", lit("kaggle:snehaanbhawal/resume-dataset"))
    .select("resume_id", "category", "raw_text", "file_path", "_ingested_at", "_source")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Escritura idempotente a Delta
# MAGIC
# MAGIC `trigger(availableNow=True)`: procesa los archivos disponibles y termina — necesario para que
# MAGIC esto funcione como una tarea de Job (no como streaming continuo). El checkpoint es lo que
# MAGIC garantiza la idempotencia: correr esta celda una segunda vez no vuelve a leer los PDFs ya
# MAGIC procesados, así que el conteo de Bronze (y de Gold, más adelante) no cambia entre corridas.

# COMMAND ----------

query = (
    bronze_stream.writeStream
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(BRONZE_TABLE)
)
query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validación rápida

# COMMAND ----------

display(spark.sql(f"""
    SELECT category, COUNT(*) AS resume_count
    FROM {BRONZE_TABLE}
    GROUP BY category
    ORDER BY resume_count DESC
"""))
