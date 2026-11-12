import json
import random

from faker import Faker

from generator.chaos import add_chaos
from generator.config import GeneratorConfig
from generator.entities import build_batch
from generator.main import run
from generator.state import GeneratorState, load_state, save_state


def _batch(state=None, **cfg):
    state = state or GeneratorState()
    fake = Faker("en_CA")
    fake.seed_instance(1)
    return state, build_batch(state, GeneratorConfig(**cfg), random.Random(1), fake)


def test_first_batch_writes_stores_once():
    state, b1 = _batch()
    _, b2 = _batch(state)
    assert len(b1["stores"]) == 4
    assert b2["stores"] == []


def test_cdc_sequence_is_unique_and_increasing():
    state, _ = _batch()
    _, b2 = _batch(state)
    seqs = [r["sequence_num"] for r in b2["customers_cdc"]]
    assert len(seqs) == len(set(seqs))
    assert seqs == sorted(seqs)  # clean data is ordered; chaos shuffles it later


def test_second_batch_contains_updates():
    state, _ = _batch()
    _, b2 = _batch(state, customer_update_rate=0.5)
    ops = {r["operation"] for r in b2["customers_cdc"]}
    assert "UPDATE" in ops


def test_late_payments_arrive_in_next_batch():
    state, b1 = _batch(late_payment_rate=1.0)
    assert b1["payments"] == []
    _, b2 = _batch(state, late_payment_rate=1.0)
    first_batch_order_ids = {o["order_id"] for o in b1["orders"]}
    assert first_batch_order_ids <= {p["order_id"] for p in b2["payments"]}


def test_schema_evolution_flag_adds_column():
    _, b = _batch(enable_loyalty_points=True)
    assert all("loyalty_points" in r for r in b["customers_cdc"])


def test_chaos_adds_duplicates_and_nulls():
    _, b = _batch()
    messy = add_chaos(b, random.Random(1), duplicate_rate=1.0, null_rate=1.0)
    assert len(messy["orders"]) == 2 * len(b["orders"])
    assert all(r["email"] is None for r in messy["customers_cdc"])


def test_state_roundtrip(tmp_path):
    s = GeneratorState(batch_count=3, sequence_num=99)
    path = str(tmp_path / "state" / "state.json")
    save_state(s, path)
    assert load_state(path) == s


def test_run_end_to_end(tmp_path):
    run(str(tmp_path), batches=2, seed=7, loyalty_points=False)
    lines = (tmp_path / "orders" / "batch_00001.json").read_text().splitlines()
    assert json.loads(lines[0])["currency"] == "CAD"
    assert load_state(str(tmp_path / "_generator_state" / "state.json")).batch_count == 2
    assert not list(tmp_path.rglob("_*.tmp"))  # no half-written files left behind
