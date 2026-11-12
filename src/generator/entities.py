"""Build one batch of business events. Pure Python: no Spark, no I/O -> easy to unit test."""

import random
from datetime import datetime, timedelta

from faker import Faker

from generator.config import GeneratorConfig
from generator.state import GeneratorState

TIERS = ["BRONZE", "SILVER", "GOLD", "PLATINUM"]
CATEGORIES = ["Electronics", "Home", "Fashion", "Sports", "Beauty", "Books"]
CARRIERS = ["Canada Post", "Purolator", "UPS", "FedEx"]
PAY_METHODS = ["CREDIT_CARD", "DEBIT", "PAYPAL", "GIFT_CARD"]
STORES = [
    {"store_id": 1, "store_name": "Online", "city": "Toronto", "region": "ON"},
    {"store_id": 2, "store_name": "Windsor Pickup", "city": "Windsor", "region": "ON"},
    {"store_id": 3, "store_name": "Montreal Pickup", "city": "Montreal", "region": "QC"},
    {"store_id": 4, "store_name": "Vancouver Pickup", "city": "Vancouver", "region": "BC"},
]


def _iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S")


def _cdc(state, op, record, ts):
    """Wrap a record in a CDC envelope, like Debezium/Fivetran would."""
    return {"operation": op, "sequence_num": state.next_seq(), "event_ts": _iso(ts), **record}


def build_batch(state: GeneratorState, cfg: GeneratorConfig, rng: random.Random, fake: Faker):
    start = datetime.fromisoformat(state.clock)
    end = start + timedelta(minutes=cfg.batch_minutes)

    def rand_ts():
        return start + timedelta(seconds=rng.randint(0, cfg.batch_minutes * 60 - 1))

    out = {k: [] for k in ["stores", "customers_cdc", "products_cdc", "orders", "payments", "shipments"]}

    # --- stores: small static reference data, written once ---------------------------
    if not state.stores_written:
        out["stores"] = [dict(s) for s in STORES]
        state.stores_written = True

    # --- customers CDC: inserts, updates, deletes --------------------------------------
    for _ in range(cfg.new_customers_per_batch):
        cid = state.next_customer_id
        state.next_customer_id += 1
        created = rand_ts()
        rec = {
            "customer_id": cid,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "city": fake.city(),
            "province": fake.province_abbr(),
            "tier": "BRONZE",
            "signup_ts": _iso(created),
        }
        if cfg.enable_loyalty_points:
            rec["loyalty_points"] = 0
        state.customers[str(cid)] = rec
        out["customers_cdc"].append(_cdc(state, "INSERT", rec, created))

    existing = list(state.customers.keys())
    for cid in rng.sample(existing, k=int(len(existing) * cfg.customer_update_rate)):
        rec = dict(state.customers[cid])
        change = rng.choice(["tier", "city", "email"])
        if change == "tier":
            rec["tier"] = TIERS[min(TIERS.index(rec["tier"]) + 1, len(TIERS) - 1)]
        elif change == "city":
            rec["city"], rec["province"] = fake.city(), fake.province_abbr()
        else:
            rec["email"] = fake.email()
        if cfg.enable_loyalty_points:
            rec["loyalty_points"] = rec.get("loyalty_points", 0) + rng.randint(10, 500)
        state.customers[cid] = rec
        out["customers_cdc"].append(_cdc(state, "UPDATE", rec, rand_ts()))

    for cid in rng.sample(existing, k=int(len(existing) * cfg.customer_delete_rate)):
        rec = state.customers.pop(cid)
        out["customers_cdc"].append(_cdc(state, "DELETE", rec, rand_ts()))

    # --- products CDC -------------------------------------------------------------------
    for _ in range(cfg.new_products_per_batch):
        pid = state.next_product_id
        state.next_product_id += 1
        rec = {
            "product_id": pid,
            "product_name": fake.catch_phrase(),
            "category": rng.choice(CATEGORIES),
            "price": round(rng.uniform(5, 500), 2),
            "description": fake.sentence(),
        }
        state.products[str(pid)] = rec
        out["products_cdc"].append(_cdc(state, "INSERT", rec, rand_ts()))

    plist = list(state.products.keys())
    for pid in rng.sample(plist, k=int(len(plist) * cfg.product_update_rate)):
        rec = dict(state.products[pid])
        if rng.random() < 0.7:
            rec["price"] = round(rec["price"] * rng.uniform(0.8, 1.2), 2)  # tracked in SCD2
        else:
            rec["description"] = fake.sentence()  # SCD1-only change
        state.products[pid] = rec
        out["products_cdc"].append(_cdc(state, "UPDATE", rec, rand_ts()))

    # --- late payments from the previous batch arrive now -----------------------------
    out["payments"].extend(state.pending_payments)
    state.pending_payments = []

    # --- orders (+ their payments) --------------------------------------------------------
    customer_ids = [int(c) for c in state.customers]
    product_ids = list(state.products.keys())
    newly_paid = []
    for _ in range(cfg.orders_per_batch if customer_ids and product_ids else 0):
        oid = state.next_order_id
        state.next_order_id += 1
        order_ts = rand_ts()
        items = []
        for pid in rng.sample(product_ids, k=rng.randint(1, min(cfg.max_items_per_order, len(product_ids)))):
            items.append(
                {"product_id": int(pid), "quantity": rng.randint(1, 3), "unit_price": state.products[pid]["price"]}
            )
        out["orders"].append(
            {
                "order_id": oid,
                "customer_id": rng.choice(customer_ids),
                "store_id": rng.choice(STORES)["store_id"],
                "order_ts": _iso(order_ts),
                "currency": "CAD",
                "items": items,
            }
        )

        success = rng.random() < cfg.payment_success_rate
        late = rng.random() < cfg.late_payment_rate
        delay = timedelta(hours=rng.uniform(2, 6)) if late else timedelta(minutes=rng.randint(1, 90))
        payment = {
            "payment_id": state.next_payment_id,
            "order_id": oid,
            "amount": round(sum(i["quantity"] * i["unit_price"] for i in items), 2),
            "method": rng.choice(PAY_METHODS),
            "status": "SUCCESS" if success else "FAILED",
            "payment_ts": _iso(order_ts + delay),
        }
        state.next_payment_id += 1
        (state.pending_payments if late else out["payments"]).append(payment)
        if success:
            newly_paid.append(oid)

    # --- shipments for orders paid in earlier batches -----------------------------------
    still_waiting = []
    for oid in state.unshipped_orders:
        if rng.random() < cfg.ship_rate:
            out["shipments"].append(
                {
                    "shipment_id": state.next_shipment_id,
                    "order_id": oid,
                    "carrier": rng.choice(CARRIERS),
                    "shipped_ts": _iso(rand_ts()),
                }
            )
            state.next_shipment_id += 1
        else:
            still_waiting.append(oid)
    state.unshipped_orders = still_waiting + newly_paid

    state.clock = _iso(end)
    state.batch_count += 1
    return out
