import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config.settings import DATA_CONFIG, PATHS
from .base import GeneratorConfig
from .restaurants import RestaurantsGenerator
from .riders import RidersGenerator
from .orders import OrdersGenerator
from .order_items import OrderItemsGenerator
from .delivery_events import DeliveryEventsGenerator
from .refunds import RefundsGenerator
from .support_tickets import SupportTicketsGenerator


def generate_all(seed: int = 42) -> None:
    config = GeneratorConfig(seed=seed)
    output_dir = PATHS["raw"]
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Generating restaurants...")
    restaurants = RestaurantsGenerator(config, DATA_CONFIG).generate()
    restaurants.to_csv(output_dir / "restaurants.csv", index=False)
    print(f"  -> {len(restaurants)} restaurants")

    print("Generating riders...")
    riders = RidersGenerator(config, DATA_CONFIG).generate()
    riders.to_csv(output_dir / "riders.csv", index=False)
    print(f"  -> {len(riders)} riders")

    print("Generating orders...")
    orders_gen = OrdersGenerator(
        config=config,
        restaurants_df=restaurants,
        date_range=(DATA_CONFIG["date_range"]["start"], DATA_CONFIG["date_range"]["end"]),
        cities=DATA_CONFIG["volumes"]["cities"],
    )
    orders_with_flags = orders_gen.generate_with_flags()
    orders = orders_with_flags.drop(columns=["_will_breach_sla"])
    orders.to_csv(output_dir / "orders.csv", index=False)
    print(f"  -> {len(orders)} orders")

    print("Generating order items...")
    order_items = OrderItemsGenerator(config, orders).generate()
    order_items.to_csv(output_dir / "order_items.csv", index=False)
    print(f"  -> {len(order_items)} order items")

    print("Generating delivery events...")
    events_gen = DeliveryEventsGenerator(
        config, orders_with_flags, riders,
        late_event_rate=DATA_CONFIG["defect_rates"]["late_event_rate"],
        orphan_rate=DATA_CONFIG["defect_rates"]["orphan_rate"],
    )
    events = events_gen.generate()
    with open(output_dir / "delivery_events.json", "w") as f:
        json.dump(events, f, indent=2)
    print(f"  -> {len(events)} delivery events")

    print("Generating refunds...")
    refunds = RefundsGenerator(config, orders).generate()
    refunds.to_csv(output_dir / "refunds.csv", index=False)
    print(f"  -> {len(refunds)} refunds")

    print("Generating support tickets...")
    tickets = SupportTicketsGenerator(config, orders).generate()
    tickets.to_csv(output_dir / "support_tickets.csv", index=False)
    print(f"  -> {len(tickets)} support tickets")

    print(f"\nAll data generated in {output_dir}")


if __name__ == "__main__":
    generate_all()
