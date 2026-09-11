# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Setup Instructions
# MAGIC %md
# MAGIC # ACRA Data.gov.sg Download
# MAGIC
# MAGIC This notebook downloads ACRA Corporate Entities datasets from data.gov.sg and stages them in UC Volume for pipeline ingestion.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC ### 1. Create Databricks Secret (one-time setup)
# MAGIC Configure through Databricks UI. Head to Catalog Explorer > corresponding schema > Create secret
# MAGIC
# MAGIC ### 2. Configure Pipeline Settings (one-time setup)
# MAGIC 1. Go to **Pipeline → Settings → Advanced → Configuration**
# MAGIC 2. Click **Add configuration**
# MAGIC 3. Add key and value. Example below, update value with corresponding secret location.
# MAGIC    - **Key:** `api_key`
# MAGIC    - **Value:** `{{secrets/projects.corporate_entities_acra.data_gov_api}}`
# MAGIC
# MAGIC ### 3. Run This Notebook
# MAGIC Execute all cells to download the latest dataset files to the UC Volume.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Configuration
import requests
import json
import time
import re
from pathlib import Path

# Configuration
COLLECTION_ID = 2
BASE_URL = "https://api-production.data.gov.sg/v2/public/api"
DOWNLOAD_URL = "https://api-open.data.gov.sg/v1/public/api"
VOLUME_PATH = "/Volumes/projects/corporate_entities_acra/data_gov_sg_raw"

# Get API key from Unity Catalog secret
# Secret location: projects.corporate_entities_acra.data_gov_api
try:
    # Access Unity Catalog secret by reading from the secret as a table
    API_KEY = dbutils.secrets.get(catalog="projects", schema="corporate_entities_acra", key="data_gov_api")
    print("✓ API key loaded successfully")
except Exception as e:
    print(f"Warning: Could not retrieve secret: {e}")
    print("Please create the secret or set API_KEY manually")
    API_KEY = None  # Set your API key here for testing: API_KEY = "your_key"

headers = {"X-API-Key": API_KEY} if API_KEY else {}

# COMMAND ----------

# DBTITLE 1,Fetch Collection Metadata
# Step 1: Get collection metadata
print(f"Fetching collection {COLLECTION_ID} metadata...")

response = requests.get(
    f"{BASE_URL}/collections/{COLLECTION_ID}/metadata",
    headers=headers
)
response.raise_for_status()

collection_data = response.json()
metadata = collection_data["data"]["collectionMetadata"]
child_datasets = metadata["childDatasets"]

# Extract lastUpdatedAt date for snapshot folder naming
last_updated = metadata["lastUpdatedAt"]  # Format: "2026-08-14T14:07:42+08:00"
snapshot_date = last_updated.split("T")[0].replace("-", "")  # Convert to YYYYMMDD

print(f"Found {len(child_datasets)} datasets in collection")
print(f"Dataset IDs: {child_datasets}")
print(f"\nCollection last updated: {last_updated}")
print(f"Snapshot folder name: {snapshot_date}")

# COMMAND ----------

# DBTITLE 1,Download All Datasets
# Step 2 & 3: For each dataset, fetch metadata and download

def exponential_backoff_retry(func, max_retries=5, initial_delay=1):
    """Retry a function with exponential backoff on rate limit errors"""
    def wrapper(*args, **kwargs):
        delay = initial_delay
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:  # Too Many Requests
                    if attempt < max_retries - 1:
                        print(f"  ⚠ Rate limit hit (attempt {attempt + 1}/{max_retries}), waiting {delay}s...")
                        time.sleep(delay)
                        delay *= 2  # Exponential backoff: 1s, 2s, 4s, 8s, 16s
                    else:
                        print(f"  ✗ Max retries reached, giving up")
                        raise
                else:
                    raise  # Re-raise non-429 errors immediately
            except Exception as e:
                raise  # Re-raise other exceptions
        return None
    return wrapper

def extract_letter_from_name(name):
    """Extract letter from dataset name like 'ACRA Information on Corporate Entities ('W')' -> 'w'"""
    match = re.search(r"\('([a-z]+)'\)", name.lower())
    if match:
        return match.group(1).lower()
    return None

@exponential_backoff_retry
def get_dataset_metadata(dataset_id):
    """Fetch dataset metadata including name and column info"""
    response = requests.get(
        f"{BASE_URL}/datasets/{dataset_id}/metadata",
        headers=headers
    )
    response.raise_for_status()
    return response.json()["data"]

@exponential_backoff_retry
def _call_initiate_download(dataset_id):
    """Call the initiate-download endpoint"""
    response = requests.get(
        f"{DOWNLOAD_URL}/datasets/{dataset_id}/initiate-download",
        headers=headers
    )
    response.raise_for_status()
    return response.json()

@exponential_backoff_retry
def _call_poll_download(dataset_id):
    """Call the poll-download endpoint"""
    response = requests.get(
        f"{DOWNLOAD_URL}/datasets/{dataset_id}/poll-download",
        headers=headers
    )
    response.raise_for_status()
    return response.json()

def download(dataset_id, max_polls=20, poll_interval=3):
    """Initiate and poll download for a dataset"""
    # Step 1: Initiate download
    print(f"  Initiating download...")
    _call_initiate_download(dataset_id)
    
    # Step 2: Poll until ready
    download_url = None
    for attempt in range(max_polls):
        poll_data = _call_poll_download(dataset_id)
        data = poll_data.get("data", {})
        download_url = data.get("url") or data.get("downloadUrl")
        
        if download_url:
            print(f"  ✓ Download ready after {attempt + 1} poll(s)")
            break
        
        if attempt < max_polls - 1:  # Don't sleep on last attempt
            time.sleep(poll_interval)
    
    if not download_url:
        raise TimeoutError(f"Download not ready after {max_polls} polls (waited {max_polls * poll_interval}s)")
    
    return download_url

# Process all datasets
download_info = []

for i, dataset_id in enumerate(child_datasets):
    print(f"\nProcessing dataset {i+1}/{len(child_datasets)}: {dataset_id}...")
    
    # Get metadata
    metadata = get_dataset_metadata(dataset_id)
    dataset_name_raw = metadata.get("name", "")
    letter = extract_letter_from_name(dataset_name_raw)
    
    if not letter:
        print(f"  WARNING: Could not extract letter from name '{dataset_name_raw}', skipping")
        continue
    
    table_name = f"acra_corporate_entities_{letter}"
    print(f"  Name: {dataset_name_raw}")
    print(f"  Table name: {table_name}")
    
    # Initiate and poll download
    download_url = download(dataset_id)
    print(f"  Download URL: {download_url}")
    
    download_info.append({
        "dataset_id": dataset_id,
        "table_name": table_name,
        "letter": letter,
        "download_url": download_url,
        "metadata": metadata
    })
    
    # Be respectful to API rate limits - pause between datasets
    time.sleep(2)  # Increased from 1s to 2s for safer rate limiting

print(f"\n✓ Initiated downloads for {len(download_info)} datasets")

# COMMAND ----------

# DBTITLE 1,Download and Save Files
# Step 4: Download files and save to UC Volume (date-based snapshots)

# Create snapshot directory using collection's lastUpdatedAt date
snapshot_dir = f"{VOLUME_PATH}/{snapshot_date}"
dbutils.fs.mkdirs(snapshot_dir)
print(f"Snapshot directory: {snapshot_dir}\n")

for info in download_info:
    table_name = info["table_name"]
    download_url = info["download_url"]
    
    print(f"Downloading {table_name}...")
    
    # Download file content
    response = requests.get(download_url, stream=True)
    response.raise_for_status()
    
    # Data.gov.sg ACRA datasets are always CSV
    file_ext = "csv"
    
    # Build file path - all files in same snapshot folder
    file_path = f"{snapshot_dir}/{table_name}.{file_ext}"
    
    # Write directly to /Volumes/ path (works on serverless)
    with open(file_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    print(f"  ✓ Saved to {file_path}")
    info["file_path"] = file_path
    info["file_format"] = file_ext

print(f"\n✓✓ All {len(download_info)} datasets downloaded to {snapshot_dir}")

# COMMAND ----------

# DBTITLE 1,Save Metadata for Pipeline
# Save dataset metadata as JSON for pipeline reference
metadata_path = f"{snapshot_dir}/_metadata.json"
metadata_summary = {
    "snapshot_date": snapshot_date,
    "collection_last_updated": last_updated,
    "datasets": [
        {
            "dataset_id": info["dataset_id"],
            "table_name": info["table_name"],
            "letter": info["letter"],
            "file_path": info["file_path"],
            "file_format": info["file_format"],
            "columns": info["metadata"].get("columns", {})
        }
        for info in download_info
    ]
}

with open(metadata_path, "w") as f:
    json.dump(metadata_summary, f, indent=2)

print(f"✓ Metadata saved to {metadata_path}")
print(f"\n{'='*60}")
print(f"OUTPUT DETAILS")
print(f"{'='*60}")
print(f"Snapshot date: {snapshot_date}")
print(f"Output 1: Datasets")
print(f"    # datasets: {len(download_info)}")
print(f"    Location: {snapshot_dir}")
print(f"Output 2: Metadata")
print(f"    Location: {metadata_path}")