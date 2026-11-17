
"""Create a source manifest for each generated batch."""

import json
import os
from datetime import datetime, timezone


def write_manifest(
    base_path: str,
    batch_id: str,
    counts: dict,
) -> str:
    """Write expected source-file counts for a completed batch."""

    manifest_dir = os.path.join(base_path, "_manifests")
    os.makedirs(manifest_dir, exist_ok=True)

    manifest_path = os.path.join(
        manifest_dir,
        f"{batch_id}.json",
    )

    # Do not silently overwrite an existing manifest.
    if os.path.exists(manifest_path):
        raise FileExistsError(
            f"Manifest already exists: {manifest_path}"
        )

    manifest = {
        "batch_id": batch_id,
        "status": "READY",
        "published_at": datetime.now(timezone.utc).isoformat(),
        "files": [
            {
                "entity": entity,
                "source_file": f"{entity}/{batch_id}.json",
                "expected_rows": count,
            }
            for entity, count in sorted(counts.items())
        ],
    }

    tmp_path = manifest_path + ".tmp"

    with open(tmp_path, "x", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    os.rename(tmp_path, manifest_path)

    return manifest_path
