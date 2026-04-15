"""Patch the onelake_security_utils Environment with the exact package version and feed connection.

Reads the package metadata produced by Pipeline A and updates the checked-in
``environment.yml`` so that the Fabric Environment installs the correct
onelake-security wheel from the target feed.

Environment variables required:
    PACKAGE_METADATA_FILE  - path to package-metadata.json from Pipeline A
    FEED_CONNECTION_ID     - Fabric Azure Artifact Feed connection ID for the target environment
"""

import json
import os
import sys
from pathlib import Path

import yaml


def main():
    metadata_file = Path(os.environ["PACKAGE_METADATA_FILE"])
    env_file = Path(
        "fabric_items/onelake_security_utils.Environment/"
        "Libraries/PublicLibraries/environment.yml"
    )

    if not metadata_file.exists():
        print(f"ERROR: metadata file not found: {metadata_file}")
        sys.exit(1)
    if not env_file.exists():
        print(f"ERROR: environment.yml not found: {env_file}")
        sys.exit(1)

    metadata = json.loads(metadata_file.read_text(encoding="utf-8"))
    package_version = metadata["packageVersion"]
    feed_connection_id = os.environ["FEED_CONNECTION_ID"]

    doc = yaml.safe_load(env_file.read_text(encoding="utf-8"))

    for dep in doc.get("dependencies", []):
        if isinstance(dep, dict) and "pip" in dep:
            updated = []
            for entry in dep["pip"]:
                if isinstance(entry, str) and entry.startswith("onelake-security"):
                    updated.append(f"onelake-security=={package_version}")
                elif isinstance(entry, str) and entry.startswith("--index-url"):
                    updated.append(f"--index-url {feed_connection_id}")
                else:
                    updated.append(entry)
            dep["pip"] = updated

    env_file.write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")
    print(f"Updated environment.yml -> onelake-security=={package_version}")
    print(f"Updated environment.yml -> --index-url {feed_connection_id}")


if __name__ == "__main__":
    main()
