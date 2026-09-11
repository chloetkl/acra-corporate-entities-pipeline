"""Bronze Layer: Consolidated ACRA Corporate Entities Ingestion

Ingests all 27 ACRA datasets (a-z + others) from data.gov.sg UC Volume snapshot
into a single bronze streaming table.

Data Source: /Volumes/projects/corporate_entities_acra/data_gov_sg_raw/{YYYYMMDD}/
Output: One bronze table with all entities + metadata columns

TODO: Make snapshot_date dynamic (read from latest folder or use pipeline parameter)
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Configuration
VOLUME_PATH = "/Volumes/projects/corporate_entities_acra/data_gov_sg_raw"
SNAPSHOT_DATE = "20260814"  # Static for now - data collection last updated date

# Source path for current snapshot (all 27 CSV files in one folder)
snapshot_path = f"{VOLUME_PATH}/{SNAPSHOT_DATE}"


@dp.table(
    name="bronze_acra_corporate_entities",
    comment=f"Bronze: All ACRA Corporate Entities from data.gov.sg snapshot {SNAPSHOT_DATE} (consolidated a-z + others)",
    table_properties={
        "quality": "bronze",
        "source": "data.gov.sg",
        "snapshot_date": SNAPSHOT_DATE
    }
)
def bronze_acra_corporate_entities():
    """
    Ingest all 27 ACRA corporate entities CSV files from UC Volume snapshot.
    Auto Loader handles schema inference and captures all columns.
    
    Files ingested:
    - acra_corporate_entities_a.csv
    - acra_corporate_entities_b.csv
    - ...
    - acra_corporate_entities_z.csv
    - acra_corporate_entities_others.csv
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")  # Let Auto Loader infer schema from CSV
        .option("rescuedDataColumn", "_rescued_data")  # Capture schema mismatches/extra columns
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Allow schema to grow
        .option("pathGlobFilter", "*.csv")  # Only read CSV files (skip _metadata.json)
        .load(snapshot_path)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_snapshot_date", F.lit(SNAPSHOT_DATE))
        # Extract letter from filename: acra_corporate_entities_a.csv -> a
        .withColumn("_dataset_letter", 
            F.regexp_extract(F.col("_metadata.file_path"), r"acra_corporate_entities_(\w+)\.csv", 1))
    )