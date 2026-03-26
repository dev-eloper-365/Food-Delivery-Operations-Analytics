import uuid
import numpy as np
import pandas as pd
from datetime import date, timedelta, datetime
from typing import Tuple, List
from .base import BaseGenerator, GeneratorConfig


class OrdersGenerator(BaseGenerator):
    """Generate synthetic order data."""

    HOUR_WEIGHTS = [
        1, 2, 3, 8, 10, 6,   # 8-13 (lunch peak)
        3, 3, 4, 7, 10, 8,   # 14-19 (dinner peak)
        6, 4, 2, 1           # 20-23
    ]

    STATUS_WEIGHTS = {"delivered": 0.85, "cancelled": 0.10, "in_progress": 0.05}
    PAYMENT_MODES = {"UPI": 0.55, "Card": 0.20, "COD": 0.15, "Wallet": 0.10}

    def __init__(
        self,
        config: GeneratorConfig,
        restaurants_df: pd.DataFrame,
        date_range: Tuple[date, date],
        cities: List[str],
        orders_per_day_range: Tuple[int, int] = (800, 1500),
        sla_breach_rate: float = 0.12,
    ):
        super().__init__(config)
        self.restaurants_df = restaurants_df
        self.start_date = date_range[0]
        self.end_date = date_range[1]
        self.cities = cities
        self.orders_per_day_range = orders_per_day_range
        self.sla_breach_rate = sla_breach_rate
        self._restaurant_by_city = restaurants_df.groupby("city", group_keys=False).apply(
            lambda x: x.to_dict("records"), include_groups=False
        ).to_dict()

    def generate(self) -> pd.DataFrame:
        """Generate orders DataFrame without internal flag columns."""
        df = self.generate_with_flags()
        return df.drop(columns=["_will_breach_sla"])

    def generate_with_flags(self) -> pd.DataFrame:
        """Generate orders DataFrame keeping internal flag columns for downstream use."""
        orders = []
        current = self.start_date

        while current <= self.end_date:
            base_volume = np.random.randint(*self.orders_per_day_range)
            dow_multiplier = 1.3 if current.weekday() >= 5 else 1.0
            daily_orders = int(base_volume * dow_multiplier)

            for _ in range(daily_orders):
                order = self._generate_single_order(current)
                orders.append(order)

            current += timedelta(days=1)

        df = pd.DataFrame(orders)
        df = self.inject_nulls(df, ["payment_mode"])
        df = self.inject_duplicates(df)
        return df

    def _generate_single_order(self, current_date: date) -> dict:
        hour = self._weighted_hour_selection()
        minute = np.random.randint(0, 60)
        second = np.random.randint(0, 60)
        order_ts = datetime(current_date.year, current_date.month, current_date.day,
                           hour, minute, second)

        city = np.random.choice(self.cities)
        restaurant = self._select_restaurant(city)

        sla_minutes = np.random.randint(30, 45)
        promised_ts = order_ts + timedelta(minutes=sla_minutes)
        will_breach = np.random.random() < self.sla_breach_rate

        status = np.random.choice(list(self.STATUS_WEIGHTS.keys()),
                                 p=list(self.STATUS_WEIGHTS.values()))
        order_value = round(np.random.lognormal(5.5, 0.6), 2)
        order_value = max(50, min(order_value, 5000))
        payment_mode = np.random.choice(list(self.PAYMENT_MODES.keys()),
                                       p=list(self.PAYMENT_MODES.values()))

        return {
            "order_id": f"ORD-{uuid.uuid4().hex[:12].upper()}",
            "customer_id": f"CUST-{np.random.randint(10000, 99999)}",
            "restaurant_id": restaurant["restaurant_id"],
            "city": city,
            "order_ts": order_ts.isoformat(),
            "promised_delivery_ts": promised_ts.isoformat(),
            "status": status,
            "order_value": order_value,
            "payment_mode": payment_mode,
            "_will_breach_sla": will_breach,
        }

    def _weighted_hour_selection(self) -> int:
        hours = list(range(8, 24))
        weights = np.array(self.HOUR_WEIGHTS) / sum(self.HOUR_WEIGHTS)
        return int(np.random.choice(hours, p=weights))

    def _select_restaurant(self, city: str) -> dict:
        city_restaurants = self._restaurant_by_city.get(city, [])
        if not city_restaurants:
            return self.restaurants_df.sample(1).to_dict("records")[0]
        return np.random.choice(city_restaurants)
