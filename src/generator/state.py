"""Generator state, persisted between runs so each batch continues the story."""

import json
import os
from dataclasses import asdict, dataclass, field


@dataclass
class GeneratorState:
    batch_count: int = 0
    clock: str = "2026-01-01T00:00:00"  # simulated "now" (ISO, UTC)
    sequence_num: int = 0  # global, monotonically increasing CDC sequence
    next_customer_id: int = 1
    next_product_id: int = 1
    next_order_id: int = 1
    next_payment_id: int = 1
    next_shipment_id: int = 1
    stores_written: bool = False
    customers: dict = field(default_factory=dict)  # customer_id -> current record
    products: dict = field(default_factory=dict)  # product_id -> current record
    pending_payments: list = field(default_factory=list)  # late payments for next batch
    unshipped_orders: list = field(default_factory=list)  # [order_id, ...] paid, not shipped

    def next_seq(self) -> int:
        self.sequence_num += 1
        return self.sequence_num


def load_state(path: str) -> GeneratorState:
    if not os.path.exists(path):
        return GeneratorState()
    with open(path) as f:
        return GeneratorState(**json.load(f))


def save_state(state: GeneratorState, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(asdict(state), f)
    os.replace(tmp, path)
