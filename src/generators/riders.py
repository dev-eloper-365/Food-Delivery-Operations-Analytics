import numpy as np
import pandas as pd
from datetime import date, timedelta
from .base import BaseGenerator, GeneratorConfig


class RidersGenerator(BaseGenerator):
    """Generate synthetic rider data."""

    SHIFT_TYPES = ["morning", "evening", "night"]
    SHIFT_WEIGHTS = [0.3, 0.5, 0.2]

    def __init__(self, config: GeneratorConfig, data_config: dict):
        super().__init__(config)
        self.cities = data_config["volumes"]["cities"]
        self.n_riders = data_config["volumes"]["riders"]

    def generate(self) -> pd.DataFrame:
        records = []
        for i in range(self.n_riders):
            city = self.cities[i % len(self.cities)]
            join_start = date(2024, 6, 1)
            join_end = date(2026, 1, 15)
            days_range = (join_end - join_start).days
            joining = join_start + timedelta(days=np.random.randint(0, days_range))

            records.append({
                "rider_id": f"RDR-{np.random.randint(10000, 99999)}",
                "city": city,
                "shift_type": np.random.choice(self.SHIFT_TYPES, p=self.SHIFT_WEIGHTS),
                "joining_date": joining.isoformat(),
            })

        df = pd.DataFrame(records)
        df = self.inject_nulls(df, ["shift_type"])
        return df
