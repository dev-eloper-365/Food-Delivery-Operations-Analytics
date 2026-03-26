import numpy as np
import pandas as pd
from datetime import date, timedelta
from .base import BaseGenerator, GeneratorConfig


class RidersGenerator(BaseGenerator):
    """Generate synthetic rider data."""

    def __init__(self, config: GeneratorConfig, data_config: dict):
        super().__init__(config)
        self.data_config = data_config
        self.n_riders = data_config["volumes"]["riders"]
        self.cities = data_config["volumes"]["cities"]
        self.shift_types = ["morning", "evening", "night"]
        self.shift_weights = [0.3, 0.5, 0.2]

    def generate(self) -> pd.DataFrame:
        n = self.n_riders

        # Generate rider IDs
        ids = [f"RDR-{np.random.randint(10000, 99999):05d}" for _ in range(n)]

        # Generate joining dates between 2024-06-01 and 2026-01-15
        start = date(2024, 6, 1)
        end = date(2026, 1, 15)
        days_range = (end - start).days
        joining_dates = [
            start + timedelta(days=int(np.random.randint(0, days_range)))
            for _ in range(n)
        ]

        df = pd.DataFrame({
            "rider_id": ids,
            "name": [self.faker.name() for _ in range(n)],
            "city": np.random.choice(self.cities, n),
            "shift_type": np.random.choice(
                self.shift_types, n, p=self.shift_weights
            ),
            "joining_date": joining_dates,
        })

        # Inject nulls
        df = self.inject_nulls(df, ["shift_type"])

        # Inject duplicates
        df = self.inject_duplicates(df)

        return df
