# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver — limpieza y enriquecimiento
# MAGIC
# MAGIC Lee `bronze_resumes` (PDFs extraídos tal cual, sin tocar) y produce `silver_resumes`: texto
# MAGIC normalizado, deduplicado, con los currículums no legibles apartados en cuarentena, y dos
# MAGIC columnas derivadas para el análisis: `word_count` y `leadership_score`.
# MAGIC
# MAGIC Escritura con `mode("overwrite")`: Silver siempre se recalcula entero desde Bronze, así que
# MAGIC correr esta celda dos veces con el mismo Bronze da exactamente el mismo resultado — esa es
# MAGIC la idempotencia acá (distinta a Bronze, que usa checkpoint porque es streaming incremental).

# COMMAND ----------

CATALOG = "recruitment_copilot"
BRONZE_TABLE = f"{CATALOG}.bronze.bronze_resumes"
SILVER_TABLE = f"{CATALOG}.silver.silver_resumes"
QUARANTINE_TABLE = f"{CATALOG}.silver.silver_resumes_quarantine"

bronze_df = spark.table(BRONZE_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Normalizar texto
# MAGIC
# MAGIC Colapsar espacios/saltos de línea repetidos que quedan como artefacto de la extracción del
# MAGIC PDF (`raw_text` en Bronze los conserva a propósito, sin tocar).

# COMMAND ----------

from pyspark.sql import functions as F

normalized_df = bronze_df.withColumn(
    "clean_text", F.trim(F.regexp_replace(F.col("raw_text"), r"\s+", " "))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Cuarentena
# MAGIC
# MAGIC Currículums donde la extracción del PDF en Bronze no produjo texto (`raw_text` nulo o vacío
# MAGIC tras normalizar) — no se puede calcular `word_count` ni `leadership_score` sobre nada, así
# MAGIC que se apartan en su propia tabla en vez de descartarse silenciosamente.

# COMMAND ----------

is_unreadable = F.col("clean_text").isNull() | (F.col("clean_text") == "")

quarantine_df = (
    normalized_df.filter(is_unreadable)
    .select("resume_id", "category", "file_path", "_ingested_at")
    .withColumn("reason", F.lit("empty_or_null_extracted_text"))
    .withColumn("_quarantined_at", F.current_timestamp())
)

readable_df = normalized_df.filter(~is_unreadable)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deduplicar
# MAGIC
# MAGIC `resume_id` (el ID del archivo en el dataset original) es la llave natural — un mismo
# MAGIC currículum no debería aparecer dos veces.

# COMMAND ----------

deduped_df = readable_df.dropDuplicates(["resume_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Columnas derivadas
# MAGIC
# MAGIC - `word_count`: longitud del currículum.
# MAGIC - `leadership_score`: heurística de palabras clave — cuenta términos de liderazgo ("led",
# MAGIC   "managed", "architected", "founded") menos términos de rol de apoyo ("assisted",
# MAGIC   "supported", "helped"), con `\b` para no matchear substrings dentro de otras palabras.

# COMMAND ----------

LEADERSHIP_TERMS = ["led", "managed", "architected", "founded"]
SUPPORT_TERMS = ["assisted", "supported", "helped"]


def term_hits(text_col, terms):
    return sum(
        F.regexp_count(F.lower(text_col), F.lit(rf"\b{term}\b")) for term in terms
    )


enriched_df = (
    deduped_df
    .withColumn("word_count", F.size(F.split(F.col("clean_text"), r"\s+")))
    .withColumn(
        "leadership_score",
        term_hits(F.col("clean_text"), LEADERSHIP_TERMS)
        - term_hits(F.col("clean_text"), SUPPORT_TERMS),
    )
    .select("resume_id", "category", "clean_text", "word_count", "leadership_score")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Escritura idempotente a Delta

# COMMAND ----------

enriched_df.write.mode("overwrite").saveAsTable(SILVER_TABLE)
quarantine_df.write.mode("overwrite").saveAsTable(QUARANTINE_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validación rápida

# COMMAND ----------

print(f"Bronze:      {bronze_df.count()} filas")
print(f"Cuarentena:  {spark.table(QUARANTINE_TABLE).count()} filas")
print(f"Silver:      {spark.table(SILVER_TABLE).count()} filas")

display(spark.sql(f"""
    SELECT category, COUNT(*) AS resume_count,
           ROUND(AVG(word_count), 1) AS avg_word_count,
           ROUND(AVG(leadership_score), 2) AS avg_leadership_score
    FROM {SILVER_TABLE}
    GROUP BY category
    ORDER BY resume_count DESC
"""))