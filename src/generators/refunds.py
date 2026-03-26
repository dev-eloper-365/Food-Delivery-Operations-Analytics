import uuid
from datetime import timedelta
import pandas as pd
import numpy as np
from .base import BaseGenerator, GeneratorConfig


class RefundsGenerator(BaseGenerator):
    REFUND_REASONS = {
        "Delay": 0.35, "Missing_Items": 0.30, "Cancellation": 0.20,
        "Wrong_Order": 0.10, "Quality_Issue": 0.05,
    }
    REFUND_PERCENTAGES = {
        "Cancellation": (1.0, 1.0), "Missing_Items": (0.2, 0.5),
        "Wrong_Order": (0.5, 1.0), "Delay": (0.1, 0.3),
        "Quality_Issue": (0.3, 0.7),
    }

    def __init__(self, config: GeneratorConfig, orders_df: pd.DataFrame, refund_rate: float = 0.08):
        super().__init__(config)
        self.orders = orders_df
        self.refund_rate = refund_rate

    def generate(self) -> pd.DataFrame:
        # Match both lowercase and capitalized status values
        status_col = self.orders["status"].str.lower()
        eligible = self.orders[status_col.isin(["delivered", "cancelled"])]

        if len(eligible) == 0:
            return pd.DataFrame(columns=["refund_id", "order_id", "refund_ts", "refund_reason", "refund_amount"])

        n_refunds = max(1, int(len(eligible) * self.refund_rate))
        refund_orders = eligible.sample(n=min(n_refunds, len(eligible)))
        refunds = [self._generate_refund(order) for _, order in refund_orders.iterrows()]
        df = pd.DataFrame(refunds)
        df = self.inject_nulls(df, ["refund_reason"])
        return df

    def _generate_refund(self, order) -> dict:
        reason = np.random.choice(list(self.REFUND_REASONS.keys()), p=list(self.REFUND_REASONS.values()))
        min_pct, max_pct = self.REFUND_PERCENTAGES[reason]
        refund_amount = round(order["order_value"] * np.random.uniform(min_pct, max_pct), 2)
        order_ts = pd.to_datetime(order["order_ts"])
        refund_ts = order_ts + timedelta(hours=np.random.randint(1, 72))
        return {
            "refund_id": f"REF-{uuid.uuid4().hex[:10].upper()}",
            "order_id": order["order_id"],
            "refund_ts": refund_ts.isoformat(),
            "refund_reason": reason,
            "refund_amount": refund_amount,
        }
