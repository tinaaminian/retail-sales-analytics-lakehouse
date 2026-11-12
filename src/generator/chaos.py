"""Make clean data realistically messy, so Silver has real work to do."""

import random

# which field gets nulled for each entity
NULLABLE_FIELD = {
    "customers_cdc": "email",
    "orders": "store_id",
    "payments": "method",
}


def add_chaos(batch: dict, rng: random.Random, duplicate_rate: float, null_rate: float) -> dict:
    messy = {}
    for entity, records in batch.items():
        records = [dict(r) for r in records]

        # 1) missing values
        field = NULLABLE_FIELD.get(entity)
        if field:
            for r in records:
                if rng.random() < null_rate:
                    r[field] = None

        # 2) duplicates (at-least-once delivery from the source)
        dups = [dict(r) for r in records if rng.random() < duplicate_rate]
        records.extend(dups)

        # 3) out-of-order arrival - file order != event order
        rng.shuffle(records)
        messy[entity] = records
    return messy
