import uuid
from datetime import timedelta
import pandas as pd
import numpy as np
from .base import BaseGenerator, GeneratorConfig


class SupportTicketsGenerator(BaseGenerator):
    TICKET_TYPES = ["Delivery_Delay", "Missing_Items", "Payment_Issue", "Wrong_Order", "App_Issue"]
    TICKET_WEIGHTS = [0.35, 0.25, 0.15, 0.15, 0.10]
    RESOLUTION_STATUSES = ["open", "resolved", "escalated"]
    RESOLUTION_WEIGHTS = [0.15, 0.70, 0.15]

    def __init__(self, config: GeneratorConfig, orders_df: pd.DataFrame, ticket_rate: float = 0.05):
        super().__init__(config)
        self.orders = orders_df
        self.ticket_rate = ticket_rate

    def generate(self) -> pd.DataFrame:
        n_tickets = int(len(self.orders) * self.ticket_rate)
        ticket_orders = self.orders.sample(n=n_tickets)
        tickets = [self._generate_ticket(order) for _, order in ticket_orders.iterrows()]
        df = pd.DataFrame(tickets)
        df = self.inject_nulls(df, ["ticket_type", "resolution_status"])
        return df

    def _generate_ticket(self, order) -> dict:
        order_ts = pd.to_datetime(order["order_ts"])
        created_ts = order_ts + timedelta(hours=np.random.randint(0, 48))
        return {
            "ticket_id": f"TKT-{uuid.uuid4().hex[:8].upper()}",
            "order_id": order["order_id"],
            "ticket_type": np.random.choice(self.TICKET_TYPES, p=self.TICKET_WEIGHTS),
            "created_ts": created_ts.isoformat(),
            "resolution_status": np.random.choice(self.RESOLUTION_STATUSES, p=self.RESOLUTION_WEIGHTS),
        }
