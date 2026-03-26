import numpy as np
import pandas as pd
from .base import BaseGenerator, GeneratorConfig


class OrderItemsGenerator(BaseGenerator):
    """Generate synthetic order item data."""

    def __init__(self, config: GeneratorConfig, orders_df: pd.DataFrame):
        super().__init__(config)
        self.orders_df = orders_df
        self.cuisine_types = [
            "North Indian", "South Indian", "Chinese", "Italian",
            "Fast Food", "Biryani", "Street Food", "Desserts",
        ]

    def generate(self) -> pd.DataFrame:
        all_items = []

        for _, order in self.orders_df.iterrows():
            # 1-5 items per order, weighted toward fewer items
            n_items = np.random.choice(
                [1, 2, 3, 4, 5], p=[0.35, 0.30, 0.20, 0.10, 0.05]
            )

            for _ in range(n_items):
                # Item price: log-normal distribution, clipped to 50-500
                item_price = round(
                    float(np.clip(np.random.lognormal(4.8, 0.5), 50, 500)), 2
                )

                # Quantity: 1-4, weighted toward 1-2
                quantity = int(np.random.choice(
                    [1, 2, 3, 4], p=[0.50, 0.30, 0.15, 0.05]
                ))

                all_items.append({
                    "order_id": order["order_id"],
                    "item_id": f"ITEM-{np.random.randint(10000000, 99999999):08d}",
                    "quantity": quantity,
                    "item_price": item_price,
                    "cuisine_type": np.random.choice(self.cuisine_types),
                })

        df = pd.DataFrame(all_items)

        # Inject nulls
        df = self.inject_nulls(df, ["quantity", "item_price"])

        # Inject duplicates
        df = self.inject_duplicates(df)

        return df
