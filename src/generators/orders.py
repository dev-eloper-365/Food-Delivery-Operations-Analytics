import numpy as np
import pandas as pd
from datetime import date, timedelta, datetime
from typing import Tuple, List
from .base import BaseGenerator, GeneratorConfig


class OrdersGenerator(BaseGenerator):
    """Generate synthetic order data."""

    def __init__(
        self,
        config: GeneratorConfig,
        restaurants_df: pd.DataFrame,
        date_range: Tuple[date, date],
        cities: List[str],
    ):
        super().__init__(config)
        self.restaurants_df = restaurants_df
        self.start_date = date_range[0]
        self.end_date = date_range[1]
        self.cities = cities
        self.statuses = ["Delivered", "Cancelled", "In_Transit", "Pending"]
        self.status_weights = [0.78, 0.08, 0.09, 0.05]

    def generate(self) -> pd.DataFrame:
        """Generate orders DataFrame without internal flag columns."""
        df = self.generate_with_flags()
        df = df.drop(columns=["_will_breach_sla"], errors="ignore")
        return df

    def generate_with_flags(self) -> pd.DataFrame:
        """Generate orders DataFrame keeping internal flag columns for downstream use."""
        days = (self.end_date - self.start_date).days
        all_orders = []

        for day_offset in range(days):
            current_date = self.start_date + timedelta(days=day_offset)
            # Vary daily volume: weekends get more orders
            weekday = current_date.weekday()
            if weekday >= 5:  # weekend
                n_orders = np.random.randint(1000, 1500)
            else:
                n_orders = np.random.randint(800, 1200)

            for _ in range(n_orders):
                # Random hour with peak weighting (lunch 11-14, dinner 18-22)
                hour_weights = np.array([
                    0.5, 0.3, 0.2, 0.1, 0.1, 0.2,   # 0-5
                    0.5, 1.0, 2.0, 3.0, 4.0, 5.0,    # 6-11
                    5.5, 5.0, 3.5, 2.5, 2.0, 3.0,    # 12-17
                    5.0, 6.0, 6.5, 5.5, 4.0, 2.0,    # 18-23
                ])
                hour_weights = hour_weights / hour_weights.sum()
                hour = np.random.choice(24, p=hour_weights)
                minute = np.random.randint(0, 60)
                second = np.random.randint(0, 60)

                order_ts = datetime.combine(
                    current_date,
                    datetime.min.time().replace(
                        hour=hour, minute=minute, second=second
                    ),
                )

                # Pick a random restaurant
                rest_idx = np.random.randint(0, len(self.restaurants_df))
                restaurant = self.restaurants_df.iloc[rest_idx]

                # Determine SLA breach flag (for delivery events)
                will_breach = np.random.random() < 0.12  # sla_breach_rate

                # Order value: log-normal distribution
                order_value = round(
                    float(np.clip(np.random.lognormal(5.5, 0.6), 100, 3000)), 2
                )

                # Delivery fee
                delivery_fee = round(
                    float(np.random.choice([0, 20, 30, 40, 50],
                                           p=[0.15, 0.25, 0.30, 0.20, 0.10])), 2
                )

                all_orders.append({
                    "order_id": f"ORD-{np.random.randint(10000000, 99999999):08d}",
                    "restaurant_id": restaurant["restaurant_id"],
                    "city": restaurant.get("city", np.random.choice(self.cities)),
                    "order_ts": order_ts.isoformat(),
                    "status": np.random.choice(
                        self.statuses, p=self.status_weights
                    ),
                    "order_value": order_value,
                    "delivery_fee": delivery_fee,
                    "_will_breach_sla": will_breach,
                })

        df = pd.DataFrame(all_orders)

        # Inject nulls on selected columns
        df = self.inject_nulls(df, ["delivery_fee", "status"])

        # Inject duplicates
        df = self.inject_duplicates(df)

        return df
