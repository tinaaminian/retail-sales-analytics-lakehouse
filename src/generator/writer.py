"""Write a batch as JSON-lines files into the landing volume."""

import json
import os


def write_batch(batch: dict, base_path: str, batch_id: str) -> dict:
    """Write each entity to <base_path>/<entity>/<batch_id>.json. Returns {entity: row_count}."""
    counts = {}
    for entity, records in batch.items():
        if not records:
            continue
        folder = os.path.join(base_path, entity)
        os.makedirs(folder, exist_ok=True)
        final = os.path.join(folder, f"{batch_id}.json")
        # Write under a name starting with "_" first: Spark/Auto Loader ignores such files,
        # so a half-written file is never ingested. Then rename into place.
        tmp = os.path.join(folder, f"_{batch_id}.json.tmp")
        with open(tmp, "w") as f:
            f.writelines(json.dumps(r) + "\n" for r in records)
        os.replace(tmp, final)
        counts[entity] = len(records)
    return counts
