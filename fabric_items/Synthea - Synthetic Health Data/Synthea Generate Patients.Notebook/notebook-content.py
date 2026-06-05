# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "699d2e49-ec6f-42ed-b0be-4ea3e2ed641b",
# META       "default_lakehouse_name": "synthea_lh",
# META       "default_lakehouse_workspace_id": "a8cbda3d-903e-4154-97d9-9a91c95abb42",
# META       "known_lakehouses": [
# META         {
# META           "id": "699d2e49-ec6f-42ed-b0be-4ea3e2ed641b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import os
import hashlib
import requests

# JAR config
synthea_version = "v3.2.0"
expected_sha256 = "REPLACE_WITH_PUBLISHED_SHA256_FOR_V3_2_0_JAR"
jar_url = f"https://github.com/synthetichealth/synthea/releases/download/{synthea_version}/synthea-with-dependencies.jar"
local_jar = "/lakehouse/default/Files/synthea/synthea-with-dependencies.jar"
meta_file = "/lakehouse/default/Files/synthea/synthea_version.txt"
temp_jar = f"{local_jar}.download"

def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()

# Determine if we need to download (file missing, version changed, or hash mismatch)
need_download = not os.path.exists(local_jar)

if not need_download and os.path.exists(meta_file):
    with open(meta_file, "r") as f:
        local_version = f.read().strip()
    if local_version != synthea_version:
        need_download = True

if not need_download and sha256_file(local_jar).lower() != expected_sha256.lower():
    need_download = True

if need_download:
    print(f"Downloading pinned Synthea JAR {synthea_version} ...")
    os.makedirs(os.path.dirname(local_jar), exist_ok=True)
    with requests.get(jar_url, stream=True) as r:
        r.raise_for_status()
        with open(temp_jar, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    downloaded_sha256 = sha256_file(temp_jar)
    if downloaded_sha256.lower() != expected_sha256.lower():
        os.remove(temp_jar)
        raise ValueError(
            f"SHA256 mismatch for downloaded JAR. Expected {expected_sha256}, got {downloaded_sha256}"
        )

    os.replace(temp_jar, local_jar)
    with open(meta_file, "w") as f:
        f.write(synthea_version)
    print("Download complete and integrity verified.")
else:
    print("Pinned JAR is present and passed integrity verification. No download needed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 3: Prepare command line arguments
# Example: java -jar synthea-with-dependencies.jar -c synthea.properties
java_args = [
    "java", "-jar", local_jar,
    "-c", "/lakehouse/default/Files/synthea/synthea.properties.yaml"
]

# Step 4: Run the JAR using subprocess and capture output
import subprocess

try:
    result = subprocess.run(java_args, capture_output=True, text=True, check=True)
    print("==== STDOUT ====")
    print(result.stdout)
    print("==== STDERR ====")
    print(result.stderr)
except subprocess.CalledProcessError as e:
    print("Error:", e)
    print("==== STDOUT ====")
    print(e.stdout)
    print("==== STDERR ====")
    print(e.stderr)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

java_args = [
    "java", "-jar", local_jar,
    # run options
    "-p", "1000",
    "-a", "30-40",
    "Massachusetts", "Boston",
    # property overrides (same keys as in synthea.properties)
    "--exporter.csv.export", "true",
    "--exporter.fhir.export", "true",
    "--exporter.baseDirectory", "/lakehouse/default/Files/synthea/output",
    "--exporter.pretty_print", "true"
]

try:
    result = subprocess.run(java_args, capture_output=True, text=True, check=True)
    print("==== STDOUT ====")
    print(result.stdout)
    print("==== STDERR ====")
    print(result.stderr)
except subprocess.CalledProcessError as e:
    print("Error:", e)
    print("==== STDOUT ====")
    print(e.stdout)
    print("==== STDERR ====")
    print(e.stderr)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# run for 
java_args = [
    "java", "-jar", local_jar,
    # run options
    "-p", "20000",
    "Massachusetts", "Boston",
    # property overrides (same keys as in synthea.properties)
    "--exporter.csv.export", "true",
    "--exporter.baseDirectory", "/lakehouse/default/Files/synthea/outputLarge",
]

try:
    result = subprocess.run(java_args, capture_output=True, text=True, check=True)
    print("==== STDOUT ====")
    print(result.stdout)
    print("==== STDERR ====")
    print(result.stderr)
except subprocess.CalledProcessError as e:
    print("Error:", e)
    print("==== STDOUT ====")
    print(e.stdout)
    print("==== STDERR ====")
    print(e.stderr)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
