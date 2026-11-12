"""Entry point: python main.py --output-path /Volumes/dev_shop/landing/raw --batches 3"""

import argparse
import inspect
import os
import random
import sys

# Make `import generator...` work when Databricks runs this file directly.
# Databricks serverless runs the script via exec(), so `__file__` is not defined there.
# The code object still knows its real path, so we read it from the current frame instead.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(inspect.currentframe().f_code.co_filename))))

from faker import Faker

from generator.chaos import add_chaos
from generator.config import GeneratorConfig
from generator.entities import build_batch
from generator.state import load_state, save_state
from generator.writer import write_batch


def run(output_path: str, batches: int, seed: int, loyalty_points: bool) -> None:
    cfg = GeneratorConfig(enable_loyalty_points=loyalty_points)
    state_path = os.path.join(output_path, "_generator_state", "state.json")
    state = load_state(state_path)

    for _ in range(batches):
        # seed + batch number -> reproducible, but every batch is different
        rng = random.Random(seed + state.batch_count)
        fake = Faker("en_CA")
        fake.seed_instance(seed + state.batch_count)

        batch_id = f"batch_{state.batch_count:05d}"
        clean = build_batch(state, cfg, rng, fake)
        messy = add_chaos(clean, rng, cfg.duplicate_rate, cfg.null_rate)
        counts = write_batch(messy, output_path, batch_id)
        save_state(state, state_path)  # save after every batch -> safe to resume
        print(f"{batch_id} (sim clock {state.clock}): {counts}")


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="ShopStream synthetic data generator")
    p.add_argument("--output-path", required=True)
    p.add_argument("--batches", type=int, default=1)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--loyalty-points", default="false", help="true = schema evolution on")
    a = p.parse_args(argv)
    a.loyalty_points = str(a.loyalty_points).lower() == "true"
    return a


if __name__ == "__main__":
    args = parse_args()
    run(args.output_path, args.batches, args.seed, args.loyalty_points)
