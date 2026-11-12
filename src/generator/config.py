"""Tunable knobs for the synthetic data generator."""

from dataclasses import dataclass


@dataclass(frozen=True)
class GeneratorConfig:
    # volume per batch
    new_customers_per_batch: int = 20
    new_products_per_batch: int = 5
    orders_per_batch: int = 100
    max_items_per_order: int = 4

    # change data capture behaviour
    customer_update_rate: float = 0.10  # share of existing customers updated per batch
    customer_delete_rate: float = 0.01
    product_update_rate: float = 0.20

    # payments / shipments
    payment_success_rate: float = 0.95
    late_payment_rate: float = 0.15  # payment arrives in the NEXT batch, 2-6h after the order
    ship_rate: float = 0.7  # share of paid orders shipped per batch

    # simulated time: each batch covers this many minutes
    batch_minutes: int = 60

    # chaos - real data is messy
    duplicate_rate: float = 0.02
    null_rate: float = 0.01

    # schema evolution switch: adds `loyalty_points` to customer records
    enable_loyalty_points: bool = False
