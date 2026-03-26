import uuid
from datetime import timedelta
from typing import List, Dict, Any
import pandas as pd
import numpy as np
from .base import BaseGenerator, GeneratorConfig


class DeliveryEventsGenerator(BaseGenerator):
    EVENT_SEQUENCE = [
        "order_confirmed", "restaurant_accepted", "food_prep_started",
        "food_ready", "rider_assigned", "rider_picked_up",
        "out_for_delivery", "delivered",
    ]

    EVENT_DELTAS = {
        "order_confirmed": (0, 0),
        "restaurant_accepted": (1, 3),
        "food_prep_started": (2, 5),
        "food_ready": (10, 25),
        "rider_assigned": (1, 5),
        "rider_picked_up": (5, 15),
        "out_for_delivery": (0, 1),
        "delivered": (10, 20),
    }

    CITY_COORDS = {
        "Mumbai": (19.076, 72.877), "Delhi": (28.613, 77.209),
        "Bangalore": (12.971, 77.594), "Chennai": (13.082, 80.270),
        "Hyderabad": (17.385, 78.486), "Pune": (18.520, 73.856),
        "Kolkata": (22.572, 88.363), "Ahmedabad": (23.022, 72.571),
    }

    def __init__(self, config: GeneratorConfig, orders_df: pd.DataFrame,
                 riders_df: pd.DataFrame, late_event_rate: float = 0.03,
                 orphan_rate: float = 0.01):
        super().__init__(config)
        self.orders = orders_df
        self.riders = riders_df
        self.late_event_rate = late_event_rate
        self.orphan_rate = orphan_rate
        self._riders_by_city = riders_df.groupby("city").apply(
            lambda x: x["rider_id"].tolist()
        ).to_dict()

    def generate(self) -> List[Dict[str, Any]]:
        events = []
        for _, order in self.orders.iterrows():
            order_events = self._generate_order_events(order)
            events.extend(order_events)
        events = self._inject_orphans(events)
        return events

    def _generate_order_events(self, order: pd.Series) -> List[Dict]:
        events = []
        order_id = order["order_id"]
        city = order.get("city", "Mumbai")
        status = order.get("status", "delivered")
        will_breach = order.get("_will_breach_sla", False)

        if status == "cancelled":
            n_events = np.random.randint(1, 4)
            event_chain = self.EVENT_SEQUENCE[:n_events] + ["cancelled"]
        else:
            event_chain = self.EVENT_SEQUENCE.copy()

        city_riders = self._riders_by_city.get(city, [])
        rider_id = np.random.choice(city_riders) if city_riders else None
        base_lat, base_lon = self.CITY_COORDS.get(city, (19.076, 72.877))
        current_ts = pd.to_datetime(order["order_ts"])

        for event_type in event_chain:
            delta = self._get_event_delta(event_type, will_breach)
            current_ts += timedelta(minutes=delta)

            event_rider_id = None
            if event_type in ["rider_assigned", "rider_picked_up",
                             "out_for_delivery", "delivered"]:
                event_rider_id = rider_id

            event = {
                "order_id": order_id,
                "rider_id": event_rider_id,
                "event_type": event_type,
                "event_ts": current_ts.isoformat(),
                "latitude": self._jitter_coord(base_lat, 0.02),
                "longitude": self._jitter_coord(base_lon, 0.02),
            }
            events.append(event)
        return events

    def _get_event_delta(self, event_type: str, will_breach: bool) -> int:
        min_delta, max_delta = self.EVENT_DELTAS.get(event_type, (1, 5))
        delta = np.random.randint(min_delta, max_delta + 1)
        if will_breach and event_type in ["food_ready", "delivered"]:
            delta += np.random.randint(10, 30)
        return delta

    def _jitter_coord(self, coord: float, range_km: float) -> float:
        jitter = np.random.uniform(-range_km * 0.01, range_km * 0.01)
        return round(coord + jitter, 6)

    def _inject_orphans(self, events: List[Dict]) -> List[Dict]:
        n_orphans = int(len(events) * self.orphan_rate)
        orphan_indices = np.random.choice(len(events), n_orphans, replace=False)
        for i in orphan_indices:
            events[i]["order_id"] = f"ORD-INVALID-{uuid.uuid4().hex[:8].upper()}"
        return events
